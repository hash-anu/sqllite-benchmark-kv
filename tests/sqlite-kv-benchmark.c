/*
** SQLite Performance Benchmark
**
** Tests: Sequential writes, random reads, sequential scan,
**        random updates, random deletes, bulk operations
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <sqlite3.h>

#define DB_FILE "benchmark_sql.db"
#define NUM_RECORDS 1000000
#define BATCH_SIZE 1000
#define NUM_READS 50000
#define NUM_UPDATES 10000
#define NUM_DELETES 5000

#define COLOR_BLUE "\x1b[34m"
#define COLOR_GREEN "\x1b[32m"
#define COLOR_YELLOW "\x1b[33m"
#define COLOR_CYAN "\x1b[36m"
#define COLOR_RESET "\x1b[0m"

/* High-resolution timer */
static double get_time(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

/* Format numbers with commas */
static void format_number(long long num, char *buf, size_t size) {
    if (num >= 1000000) {
        snprintf(buf, size, "%lld,%03lld,%03lld", 
                 num/1000000, (num/1000)%1000, num%1000);
    } else if (num >= 1000) {
        snprintf(buf, size, "%lld,%03lld", num/1000, num%1000);
    } else {
        snprintf(buf, size, "%lld", num);
    }
}

static void print_result(const char *test, double elapsed, int ops) {
    double ops_per_sec = ops / elapsed;
    char buf[32];
    format_number((long long)ops_per_sec, buf, sizeof(buf));
    
    printf("  %-30s: ", test);
    printf(COLOR_GREEN "%s ops/sec" COLOR_RESET " ", buf);
    printf("(%.3f seconds for %d ops)\n", elapsed, ops);
}

static void print_header(const char *title) {
    printf("\n" COLOR_CYAN);
    printf("════════════════════════════════════════════════════════\n");
    printf("  %s\n", title);
    printf("════════════════════════════════════════════════════════\n");
    printf(COLOR_RESET);
}

/* Execute SQL without result */
static int exec_sql(sqlite3 *db, const char *sql) {
    char *err_msg = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }
    return 0;
}

/* Initialize database with table */
static int init_database(sqlite3 *db) {
    const char *create_table = 
    "CREATE TABLE IF NOT EXISTS kvpairs ("
    "  key BLOB PRIMARY KEY,"
    "  value BLOB NOT NULL"
    ") WITHOUT ROWID";
    if (exec_sql(db, create_table) != 0) return -1;
    
    /* Enable optimizations */
  //  exec_sql(db, "PRAGMA synchronous = NORMAL");
  //  exec_sql(db, "PRAGMA journal_mode = WAL");
  //  exec_sql(db, "PRAGMA cache_size = -64000");
  //  exec_sql(db, "PRAGMA temp_store = MEMORY");
    exec_sql(db, "PRAGMA page_size = 4096");
    exec_sql(db, "PRAGMA cache_size = 2000");
exec_sql(db, "PRAGMA journal_mode = WAL"); /* match rollback journal */
exec_sql(db, "PRAGMA synchronous = FULL");    /* fair durability */

    return 0;
}

/* ==================== BENCHMARK 1: Sequential Writes ==================== */
static void bench_sequential_writes(sqlite3 *db) {
    print_header("BENCHMARK 1: Sequential Writes");
    printf("  Writing %d records in batches of %d...\n\n", NUM_RECORDS, BATCH_SIZE);
    
    char sql[256];
    int i, rc;
    double start, end;
    sqlite3_stmt *stmt = NULL;
    
    /* Prepare statement */
    const char *insert_sql = "INSERT OR REPLACE INTO kvpairs (key, value) VALUES (?, ?)";
    rc = sqlite3_prepare_v2(db, insert_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return;
    }
    
    start = get_time();
    
    for (i = 0; i < NUM_RECORDS; i++) {
        if (i % BATCH_SIZE == 0) {
            if (i > 0) {
                exec_sql(db, "COMMIT");
            }
            exec_sql(db, "BEGIN TRANSACTION");
        }
        
        snprintf(sql, sizeof(sql), "key_%08d", i);
        sqlite3_bind_blob(stmt, 1, sql, strlen(sql), SQLITE_TRANSIENT);
        
        snprintf(sql, sizeof(sql), "value_%08d_with_some_additional_data_to_make_it_realistic", i);
        sqlite3_bind_blob(stmt, 2, sql, strlen(sql), SQLITE_TRANSIENT);
        
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    
    exec_sql(db, "COMMIT");
    
    end = get_time();
    
    sqlite3_finalize(stmt);
    
    print_result("Sequential writes", end - start, NUM_RECORDS);
}

/* ==================== BENCHMARK 2: Random Reads ==================== */
static void bench_random_reads(sqlite3 *db) {
    print_header("BENCHMARK 2: Random Reads");
    printf("  Reading %d random records...\n\n", NUM_READS);
    
    char key[32];
    int i, rc;
    double start, end;
    sqlite3_stmt *stmt = NULL;
    
    const char *select_sql = "SELECT value FROM kvpairs WHERE key = ?";
    rc = sqlite3_prepare_v2(db, select_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return;
    }
    
    start = get_time();
    
    for (i = 0; i < NUM_READS; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);
        
        sqlite3_bind_blob(stmt, 1, key, strlen(key), SQLITE_TRANSIENT);
        
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            /* Got the value - just consume it */
            sqlite3_column_blob(stmt, 0);
        }
        
        sqlite3_reset(stmt);
    }
    
    end = get_time();
    
    sqlite3_finalize(stmt);
    
    print_result("Random reads", end - start, NUM_READS);
}

/* ==================== BENCHMARK 3: Sequential Scan ==================== */
static void bench_sequential_scan(sqlite3 *db) {
    print_header("BENCHMARK 3: Sequential Scan");
    printf("  Scanning all records...\n\n");
    
    int count = 0, rc;
    double start, end;
    sqlite3_stmt *stmt = NULL;
    
    const char *scan_sql = "SELECT key, value FROM kvpairs ORDER BY key";
    rc = sqlite3_prepare_v2(db, scan_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return;
    }
    
    start = get_time();
    
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        /* Access key and value to simulate real work */
        sqlite3_column_blob(stmt, 0);
        sqlite3_column_blob(stmt, 1);
        count++;
    }
    
    end = get_time();
    
    sqlite3_finalize(stmt);
    
    print_result("Sequential scan", end - start, count);
}

/* ==================== BENCHMARK 4: Random Updates ==================== */
static void bench_random_updates(sqlite3 *db) {
    print_header("BENCHMARK 4: Random Updates");
    printf("  Updating %d random records...\n\n", NUM_UPDATES);
    
    char key[32], value[128];
    int i, rc;
    double start, end;
    sqlite3_stmt *stmt = NULL;
    
    const char *update_sql = "UPDATE kvpairs SET value = ? WHERE key = ?";
    rc = sqlite3_prepare_v2(db, update_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return;
    }
    
    exec_sql(db, "BEGIN TRANSACTION");
    
    start = get_time();
    
    for (i = 0; i < NUM_UPDATES; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);
        snprintf(value, sizeof(value), "updated_value_%08d", idx);
        
        sqlite3_bind_blob(stmt, 1, value, strlen(value), SQLITE_TRANSIENT);
        sqlite3_bind_blob(stmt, 2, key, strlen(key), SQLITE_TRANSIENT);
        
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    
    exec_sql(db, "COMMIT");
    
    end = get_time();
    
    sqlite3_finalize(stmt);
    
    print_result("Random updates", end - start, NUM_UPDATES);
}

/* ==================== BENCHMARK 5: Random Deletes ==================== */
static void bench_random_deletes(sqlite3 *db) {
    print_header("BENCHMARK 5: Random Deletes");
    printf("  Deleting %d random records...\n\n", NUM_DELETES);
    
    char key[32];
    int i, rc;
    double start, end;
    sqlite3_stmt *stmt = NULL;
    
    const char *delete_sql = "DELETE FROM kvpairs WHERE key = ?";
    rc = sqlite3_prepare_v2(db, delete_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return;
    }
    
    exec_sql(db, "BEGIN TRANSACTION");
    
    start = get_time();
    
    for (i = 0; i < NUM_DELETES; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);
        
        sqlite3_bind_blob(stmt, 1, key, strlen(key), SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    
    exec_sql(db, "COMMIT");
    
    end = get_time();
    
    sqlite3_finalize(stmt);
    
    print_result("Random deletes", end - start, NUM_DELETES);
}

/* ==================== BENCHMARK 6: Exists Checks ==================== */
static void bench_exists_checks(sqlite3 *db) {
    print_header("BENCHMARK 6: Exists Checks");
    printf("  Checking existence of %d keys...\n\n", NUM_READS);
    
    char key[32];
    int i, rc;
    double start, end;
    sqlite3_stmt *stmt = NULL;
    
    const char *exists_sql = "SELECT 1 FROM kvpairs WHERE key = ? LIMIT 1";
    rc = sqlite3_prepare_v2(db, exists_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return;
    }
    
    start = get_time();
    
    for (i = 0; i < NUM_READS; i++) {
        int idx = rand() % NUM_RECORDS;
        snprintf(key, sizeof(key), "key_%08d", idx);
        
        sqlite3_bind_blob(stmt, 1, key, strlen(key), SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    
    end = get_time();
    
    sqlite3_finalize(stmt);
    
    print_result("Exists checks", end - start, NUM_READS);
}

/* ==================== BENCHMARK 7: Mixed Workload ==================== */
static void bench_mixed_workload(sqlite3 *db) {
    print_header("BENCHMARK 7: Mixed Workload");
    printf("  70%% reads, 20%% writes, 10%% deletes...\n\n");
    
    int total_ops = 20000;
    char key[32], value[128];
    int i, rc;
    double start, end;
    sqlite3_stmt *select_stmt = NULL;
    sqlite3_stmt *update_stmt = NULL;
    sqlite3_stmt *delete_stmt = NULL;
    
    /* Prepare statements */
    sqlite3_prepare_v2(db, "SELECT value FROM kvpairs WHERE key = ?", -1, &select_stmt, NULL);
    sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO kvpairs (key, value) VALUES (?, ?)", -1, &update_stmt, NULL);
    sqlite3_prepare_v2(db, "DELETE FROM kvpairs WHERE key = ?", -1, &delete_stmt, NULL);
    
    exec_sql(db, "BEGIN TRANSACTION");
    
    start = get_time();
    
    for (i = 0; i < total_ops; i++) {
        int idx = rand() % NUM_RECORDS;
        int op = rand() % 100;
        
        snprintf(key, sizeof(key), "key_%08d", idx);
        
        if (op < 70) {
            /* Read */
            sqlite3_bind_blob(select_stmt, 1, key, strlen(key), SQLITE_TRANSIENT);
            if (sqlite3_step(select_stmt) == SQLITE_ROW) {
                sqlite3_column_blob(select_stmt, 0);
            }
            sqlite3_reset(select_stmt);
        } else if (op < 90) {
            /* Write */
            snprintf(value, sizeof(value), "mixed_value_%08d", idx);
            sqlite3_bind_blob(update_stmt, 1, key, strlen(key), SQLITE_TRANSIENT);
            sqlite3_bind_blob(update_stmt, 2, value, strlen(value), SQLITE_TRANSIENT);
            sqlite3_step(update_stmt);
            sqlite3_reset(update_stmt);
        } else {
            /* Delete */
            sqlite3_bind_blob(delete_stmt, 1, key, strlen(key), SQLITE_TRANSIENT);
            sqlite3_step(delete_stmt);
            sqlite3_reset(delete_stmt);
        }
    }
    
    exec_sql(db, "COMMIT");
    
    end = get_time();
    
    sqlite3_finalize(select_stmt);
    sqlite3_finalize(update_stmt);
    sqlite3_finalize(delete_stmt);
    
    print_result("Mixed workload", end - start, total_ops);
}

/* ==================== BENCHMARK 8: Bulk Insert ==================== */
static void bench_bulk_insert(void) {
    print_header("BENCHMARK 8: Bulk Insert (Single Transaction)");
    printf("  Inserting %d records in one transaction...\n\n", NUM_RECORDS);
    
    sqlite3 *db = NULL;
    char key[32], value[128];
    int i, rc;
    double start, end;
    sqlite3_stmt *stmt = NULL;
    
    remove("benchmark_bulk.db");
    
    rc = sqlite3_open("benchmark_bulk.db", &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        return;
    }
    
    init_database(db);
    
    const char *insert_sql = "INSERT INTO kvpairs (key, value) VALUES (?, ?)";
    sqlite3_prepare_v2(db, insert_sql, -1, &stmt, NULL);
    
    exec_sql(db, "BEGIN TRANSACTION");
    
    start = get_time();
    
    for (i = 0; i < NUM_RECORDS; i++) {
        snprintf(key, sizeof(key), "bulk_key_%08d", i);
        snprintf(value, sizeof(value), "bulk_value_%08d", i);
        
        sqlite3_bind_blob(stmt, 1, key, strlen(key), SQLITE_TRANSIENT);
        sqlite3_bind_blob(stmt, 2, value, strlen(value), SQLITE_TRANSIENT);
        
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    
    exec_sql(db, "COMMIT");
    
    end = get_time();
    
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    remove("benchmark_bulk.db");
    
    print_result("Bulk insert", end - start, NUM_RECORDS);
}

/* ==================== Main ==================== */
int main(void) {
    sqlite3 *db = NULL;
    double total_start, total_end;
    int rc;
    
    printf("\n");
    printf(COLOR_BLUE "╔══════════════════════════════════════════════════════════════╗\n");
    printf("║          SQLite Performance Benchmark                        ║\n");
    printf("║                                                              ║\n");
    printf("║  Database: %-50s║\n", DB_FILE);
    printf("║  Records:  %-50d║\n", NUM_RECORDS);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf(COLOR_RESET);
    
    srand(time(NULL));
    
    /* Initialize database */
    printf("\n" COLOR_YELLOW "Initializing database..." COLOR_RESET "\n");
    remove(DB_FILE);
    
    rc = sqlite3_open(DB_FILE, &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    
    if (init_database(db) != 0) {
        fprintf(stderr, "Failed to initialize database\n");
        sqlite3_close(db);
        return 1;
    }
    
    total_start = get_time();
    
    /* Run benchmarks */
    bench_sequential_writes(db);
    bench_random_reads(db);
    bench_sequential_scan(db);
    bench_random_updates(db);
    bench_random_deletes(db);
    bench_exists_checks(db);
    bench_mixed_workload(db);
    
    /* Get database stats */
    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM kvpairs", -1, &stmt, NULL);
    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        printf("  Records remaining: %d\n", sqlite3_column_int(stmt, 0));
    }
    sqlite3_finalize(stmt);
    
    /* Get page count and size */
    rc = sqlite3_prepare_v2(db, "PRAGMA page_count", -1, &stmt, NULL);
    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        int page_count = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
        
        rc = sqlite3_prepare_v2(db, "PRAGMA page_size", -1, &stmt, NULL);
        if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
            int page_size = sqlite3_column_int(stmt, 0);
            printf("  Database size: %.2f MB (%d pages × %d bytes)\n",
                   (page_count * page_size) / (1024.0 * 1024.0),
                   page_count, page_size);
        }
    }
    sqlite3_finalize(stmt);
    
    sqlite3_close(db);
    
    /* Run bulk insert test */
    bench_bulk_insert();
    total_end = get_time();
    
    /* Summary */
    printf("\n" COLOR_CYAN);
    printf("════════════════════════════════════════════════════════\n");
    printf("  SUMMARY\n");
    printf("════════════════════════════════════════════════════════\n");
    printf(COLOR_RESET);
    printf("  Total benchmark time: " COLOR_GREEN "%.2f seconds" COLOR_RESET "\n", 
           total_end - total_start);
    
    
    printf("\n" COLOR_GREEN "✓ Benchmark complete!" COLOR_RESET "\n\n");
    
    /* Cleanup */
    remove(DB_FILE);
    
    return 0;
}
