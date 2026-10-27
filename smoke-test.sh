#!/usr/bin/env bash
set -euo pipefail

STARTUP_TIMEOUT="${STARTUP_TIMEOUT:-120}"
JOB_TIMEOUT="${JOB_TIMEOUT:-120}"
SKIP_BUILD="${SKIP_BUILD:-false}"
FLINK_URL="http://localhost:8081"
PROJECT="etl-template-with-flink-smoke-test"
JAR_ID="etl-template-with-flink.jar"
ENTRY_CLASS="template.job.ItemsEtlJob"
CONTAINERS=(mongo postgres etl-template-with-flink-jobmanager etl-template-with-flink-taskmanager)

# items expected in MongoDB after the job, built from src/test/resources/sql
EXPECTED_ITEMS=(
    "item-a|Item A|Test item A|part-a1/Part A1,part-a2/Part A2,part-a3/Part A3"
    "item-b|Item B|Test item B|part-b1/Part B1,part-b2/Part B2"
    "item-c|Item C|Test item C|part-c1/Part C1"
    "item-d|Item D|Test item D|"
)

cd "$(dirname "$0")"

read_env() {
    local name="$1"
    if [[ -n "${!name:-}" ]]; then
        echo "${!name}"
    else
        grep "^${name}=" .env | cut -d= -f2- | tr -d '\r'
    fi
}

MONGO_PASSWORD="$(read_env ETL_TEMPLATE_MONGO_PASSWORD)"
POSTGRES_PASSWORD="$(read_env ETL_TEMPLATE_POSTGRESQL_PASSWORD)"
STARTED=false
CHECKS=0

log() {
    echo "[smoke-test] $*"
}

fail() {
    echo "[smoke-test] FAILED: $*" >&2
    print_logs
    exit 1
}

compose() {
    docker compose -p "$PROJECT" "$@"
}

print_logs() {
    if [[ "$STARTED" == "true" ]]; then
        echo "----- container logs -----" >&2
        compose logs --tail 50 >&2 || true
    fi
}

cleanup() {
    if [[ "$STARTED" == "true" ]]; then
        log "Stopping containers..."
        compose down -v --remove-orphans >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

pass() {
    CHECKS=$((CHECKS + 1))
    log "OK  $*"
}

request() {
    local method="$1" path="$2"
    local response
    response="$(curl -s -w "\n%{http_code}" -X "$method" "${FLINK_URL}${path}")" || fail "$method $path - connection error"
    STATUS="${response##*$'\n'}"
    BODY="${response%$'\n'*}"
}

expect() {
    local expected_status="$1" method="$2" path="$3"
    shift 3
    request "$method" "$path"
    [[ "$STATUS" == "$expected_status" ]] || fail "$method $path - expected status $expected_status but was $STATUS, body: $BODY"
    local fragment
    for fragment in "$@"; do
        [[ "$BODY" == *"$fragment"* ]] || fail "$method $path - expected body to contain '$fragment' but was: $BODY"
    done
    pass "$method $path -> $STATUS"
}

wait_for() {
    local description="$1"
    shift
    local deadline=$((SECONDS + STARTUP_TIMEOUT))
    until "$@" &>/dev/null; do
        if (( SECONDS >= deadline )); then
            fail "$description not ready within ${STARTUP_TIMEOUT}s"
        fi
        sleep 2
    done
    pass "$description ready after ${SECONDS}s"
}

postgres_query() {
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" postgres psql -h 127.0.0.1 -U admin -d testdb -tAc "$1"
}

mongo_eval() {
    docker exec mongo mongosh "mongodb://admin:${MONGO_PASSWORD}@localhost:27017/?authSource=admin" --quiet --eval "$1"
}

postgres_seeded() {
    [[ "$(postgres_query "SELECT count(*) FROM parts")" == "6" ]]
}

mongo_ready() {
    [[ "$(mongo_eval "db.runCommand({ ping: 1 }).ok")" == "1" ]]
}

flink_ready() {
    local overview
    overview="$(curl -sf "${FLINK_URL}/overview")" || return 1
    [[ "$overview" == *'"taskmanagers":1'* && "$overview" != *'"slots-available":0'* ]]
}

build_jar() {
    if [[ "$SKIP_BUILD" == "true" ]]; then
        log "Skipping build"
    else
        log "Building application..."
        ./mvnw -B -q clean package -DskipTests
    fi
    ls target/etl-template-with-flink-*.jar &>/dev/null || fail "no JAR found in target directory"
}

start_environment() {
    command -v docker >/dev/null || fail "docker is not installed"
    local container
    for container in "${CONTAINERS[@]}"; do
        if docker container inspect "$container" &>/dev/null; then
            fail "container '$container' already exists, stop it first (e.g. with 'docker compose down')"
        fi
    done

    log "Starting Flink cluster, PostgreSQL and MongoDB..."
    STARTED=true
    compose up -d --build --quiet-pull >/dev/null 2>&1 || fail "docker compose up failed"
    SECONDS=0
    wait_for "PostgreSQL with source data" postgres_seeded
    wait_for "MongoDB" mongo_ready
    wait_for "Flink cluster" flink_ready
}

check_cluster() {
    # Flink dashboard and REST API
    expect 200 GET / "Apache Flink Web Dashboard"
    expect 200 GET /config '"flink-version":"'

    # Flink containers run as non-root user
    local container user
    for container in etl-template-with-flink-jobmanager etl-template-with-flink-taskmanager; do
        user="$(docker exec "$container" whoami)"
        [[ "$user" == "flink" ]] || fail "$container runs as '$user' instead of 'flink'"
        pass "$container runs as non-root user '$user'"
    done

    # MongoDB starts without transformed items
    [[ "$(mongo_eval 'db.getSiblingDB("local").items.countDocuments()')" == "0" ]] || fail "items collection is not empty before the job"
    pass "items collection is empty before the job"
}

run_job() {
    expect 200 POST "/jars/${JAR_ID}/run?entry-class=${ENTRY_CLASS}" '"jobid":"'
    local job_id state
    job_id="$(sed -E 's/.*"jobid":"([^"]+)".*/\1/' <<<"$BODY")"

    local deadline=$((SECONDS + JOB_TIMEOUT))
    while true; do
        request GET "/jobs/${job_id}"
        state="$(grep -oE '"state":"[A-Z_]+"' <<<"$BODY" | head -n 1 | cut -d'"' -f4)"
        case "$state" in
            FINISHED) break ;;
            FAILED|CANCELED|SUSPENDED)
                request GET "/jobs/${job_id}/exceptions"
                fail "job $job_id ended with state $state, exceptions: $BODY" ;;
        esac
        if (( SECONDS >= deadline )); then
            fail "job $job_id did not finish within ${JOB_TIMEOUT}s (state: ${state:-unknown})"
        fi
        sleep 2
    done
    pass "job $job_id finished"
}

check_items() {
    local actual expected
    actual="$(mongo_eval '
        db.getSiblingDB("local").items.find().sort({ _id: 1 }).toArray()
            .map(i => [i._id, i.name, i.description, (i.parts || []).map(p => p.partId + "/" + p.name).sort().join(",")].join("|"))
            .join("\n")
    ' | tr -d '\r')"
    expected="$(printf '%s\n' "${EXPECTED_ITEMS[@]}")"
    [[ "$actual" == "$expected" ]] || fail "unexpected items in MongoDB, expected:
$expected
but was:
$actual"
    pass "MongoDB contains ${#EXPECTED_ITEMS[@]} transformed items"
}

build_jar
start_environment
check_cluster

log "Running ETL job..."
run_job
check_items

log "Running ETL job again to verify upserts..."
run_job
check_items

log "PASSED ($CHECKS checks)"
