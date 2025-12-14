#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sqlite3.h>

#define NUM_OPERATIONS 10000
#define BATCH_SIZE 1000
#define KEY_SIZE 32
#define VALUE_SIZE 128

typedef struct {
    double total_time;
    double min_time;
    double max_time;
    double avg_time;
    int count;
} BenchmarkStats;

// Utility function to get current time in microseconds
double get_time_us() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000.0 + ts.tv_nsec / 1000.0;
}

// Generate random string
void generate_random_string(char *str, size_t length) {
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (size_t i = 0; i < length - 1; i++) {
        str[i] = charset[rand() % (sizeof(charset) - 1)];
    }
    str[length - 1] = '\0';
}

// Print statistics
void print_stats(const char *operation, BenchmarkStats *stats) {
    printf("\n%s Results:\n", operation);
    printf("  Total operations: %d\n", stats->count);
    printf("  Total time: %.2f ms\n", stats->total_time / 1000.0);
    printf("  Average time: %.2f µs\n", stats->avg_time);
    printf("  Min time: %.2f µs\n", stats->min_time);
    printf("  Max time: %.2f µs\n", stats->max_time);
    printf("  Throughput: %.2f ops/sec\n", 1000000.0 / stats->avg_time);
    printf("----------------------------------------\n");
}

// Initialize database
int init_database(sqlite3 **db, const char *db_path) {
    int rc = sqlite3_open(db_path, db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(*db));
        return rc;
    }
    
    printf("SQLite version: %s\n", sqlite3_libversion());
    
    const char *create_table = 
        "CREATE TABLE IF NOT EXISTS kv_store ("
        "key TEXT PRIMARY KEY, "
        "value TEXT NOT NULL)";
    
    char *err_msg = NULL;
    rc = sqlite3_exec(*db, create_table, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }
    
    // Create index
    rc = sqlite3_exec(*db, "CREATE INDEX IF NOT EXISTS idx_key ON kv_store(key)", 
                      NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
    }
    
    return rc;
}

// Benchmark: Single INSERT operations (auto-commit)
void benchmark_single_insert(sqlite3 *db, int count) {
    printf("\n=== Benchmarking %d Individual INSERTs (Auto-commit) ===\n", count);
    
    BenchmarkStats stats = {0, 1e9, 0, 0, count};
    sqlite3_stmt *stmt;
    
    const char *sql = "INSERT INTO kv_store (key, value) VALUES (?, ?)";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    char key[KEY_SIZE];
    char value[VALUE_SIZE];
    
    for (int i = 0; i < count; i++) {
        snprintf(key, KEY_SIZE, "single_key_%08d", i);
        generate_random_string(value, VALUE_SIZE);
        
        double start = get_time_us();
        
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, value, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
        
        double elapsed = get_time_us() - start;
        stats.total_time += elapsed;
        if (elapsed < stats.min_time) stats.min_time = elapsed;
        if (elapsed > stats.max_time) stats.max_time = elapsed;
    }
    
    sqlite3_finalize(stmt);
    stats.avg_time = stats.total_time / count;
    print_stats("Single INSERT", &stats);
}

// Benchmark: Batch INSERT with transaction
void benchmark_batch_insert(sqlite3 *db, int count, int batch_size) {
    printf("\n=== Benchmarking Batch INSERT (%d records, batch size %d) ===\n", 
           count, batch_size);
    
    BenchmarkStats stats = {0, 0, 0, 0, 1};
    sqlite3_stmt *stmt;
    
    const char *sql = "INSERT INTO kv_store (key, value) VALUES (?, ?)";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    char key[KEY_SIZE];
    char value[VALUE_SIZE];
    
    double start = get_time_us();
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    
    for (int i = 0; i < count; i++) {
        snprintf(key, KEY_SIZE, "batch_key_%08d", i);
        generate_random_string(value, VALUE_SIZE);
        
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, value, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
        
        if ((i + 1) % batch_size == 0) {
            sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
            sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
        }
    }
    
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    double elapsed = get_time_us() - start;
    
    sqlite3_finalize(stmt);
    
    stats.total_time = elapsed;
    stats.avg_time = elapsed / count;
    stats.min_time = stats.avg_time;
    stats.max_time = stats.avg_time;
    stats.count = count;
    
    print_stats("Batch INSERT", &stats);
}

// Benchmark: GET operations
void benchmark_get(sqlite3 *db, int count) {
    printf("\n=== Benchmarking %d GET Operations ===\n", count);
    
    BenchmarkStats stats = {0, 1e9, 0, 0, count};
    sqlite3_stmt *stmt;
    
    const char *sql = "SELECT value FROM kv_store WHERE key = ?";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    char key[KEY_SIZE];
    
    for (int i = 0; i < count; i++) {
        snprintf(key, KEY_SIZE, "batch_key_%08d", rand() % count);
        
        double start = get_time_us();
        
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        int rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW) {
            // Read the value
            const unsigned char *value = sqlite3_column_text(stmt, 0);
            (void)value; // Suppress unused warning
        }
        sqlite3_reset(stmt);
        
        double elapsed = get_time_us() - start;
        stats.total_time += elapsed;
        if (elapsed < stats.min_time) stats.min_time = elapsed;
        if (elapsed > stats.max_time) stats.max_time = elapsed;
    }
    
    sqlite3_finalize(stmt);
    stats.avg_time = stats.total_time / count;
    print_stats("GET", &stats);
}

// Benchmark: UPDATE operations
void benchmark_update(sqlite3 *db, int count) {
    printf("\n=== Benchmarking %d UPDATE Operations ===\n", count);
    
    BenchmarkStats stats = {0, 1e9, 0, 0, count};
    sqlite3_stmt *stmt;
    
    const char *sql = "UPDATE kv_store SET value = ? WHERE key = ?";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    char key[KEY_SIZE];
    char value[VALUE_SIZE];
    
    for (int i = 0; i < count; i++) {
        snprintf(key, KEY_SIZE, "batch_key_%08d", rand() % count);
        generate_random_string(value, VALUE_SIZE);
        
        double start = get_time_us();
        
        sqlite3_bind_text(stmt, 1, value, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, key, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
        
        double elapsed = get_time_us() - start;
        stats.total_time += elapsed;
        if (elapsed < stats.min_time) stats.min_time = elapsed;
        if (elapsed > stats.max_time) stats.max_time = elapsed;
    }
    
    sqlite3_finalize(stmt);
    stats.avg_time = stats.total_time / count;
    print_stats("UPDATE", &stats);
}

// Benchmark: DELETE operations
void benchmark_delete(sqlite3 *db, int count) {
    printf("\n=== Benchmarking %d DELETE Operations ===\n", count);
    
    BenchmarkStats stats = {0, 1e9, 0, 0, count};
    sqlite3_stmt *stmt;
    
    const char *sql = "DELETE FROM kv_store WHERE key = ?";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    char key[KEY_SIZE];
    
    for (int i = 0; i < count; i++) {
        snprintf(key, KEY_SIZE, "batch_key_%08d", i);
        
        double start = get_time_us();
        
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
        
        double elapsed = get_time_us() - start;
        stats.total_time += elapsed;
        if (elapsed < stats.min_time) stats.min_time = elapsed;
        if (elapsed > stats.max_time) stats.max_time = elapsed;
    }
    
    sqlite3_finalize(stmt);
    stats.avg_time = stats.total_time / count;
    print_stats("DELETE", &stats);
}

// Benchmark: Transaction with mixed operations
void benchmark_mixed_transaction(sqlite3 *db, int count) {
    printf("\n=== Benchmarking Mixed Transaction (%d ops: INSERT/UPDATE/SELECT) ===\n", count);
    
    char key[KEY_SIZE];
    char value[VALUE_SIZE];
    
    double start = get_time_us();
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    
    sqlite3_stmt *insert_stmt, *update_stmt, *select_stmt;
    sqlite3_prepare_v2(db, "INSERT INTO kv_store (key, value) VALUES (?, ?)", 
                       -1, &insert_stmt, NULL);
    sqlite3_prepare_v2(db, "UPDATE kv_store SET value = ? WHERE key = ?", 
                       -1, &update_stmt, NULL);
    sqlite3_prepare_v2(db, "SELECT value FROM kv_store WHERE key = ?", 
                       -1, &select_stmt, NULL);
    
    for (int i = 0; i < count; i++) {
        int op = rand() % 3;
        snprintf(key, KEY_SIZE, "mixed_key_%08d", rand() % count);
        generate_random_string(value, VALUE_SIZE);
        
        switch(op) {
            case 0: // INSERT
                sqlite3_bind_text(insert_stmt, 1, key, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(insert_stmt, 2, value, -1, SQLITE_TRANSIENT);
                sqlite3_step(insert_stmt);
                sqlite3_reset(insert_stmt);
                break;
            case 1: // UPDATE
                sqlite3_bind_text(update_stmt, 1, value, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(update_stmt, 2, key, -1, SQLITE_TRANSIENT);
                sqlite3_step(update_stmt);
                sqlite3_reset(update_stmt);
                break;
            case 2: // SELECT
                sqlite3_bind_text(select_stmt, 1, key, -1, SQLITE_TRANSIENT);
                sqlite3_step(select_stmt);
                sqlite3_reset(select_stmt);
                break;
        }
    }
    
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    double elapsed = get_time_us() - start;
    
    sqlite3_finalize(insert_stmt);
    sqlite3_finalize(update_stmt);
    sqlite3_finalize(select_stmt);
    
    BenchmarkStats stats = {elapsed, elapsed/count, elapsed/count, elapsed/count, count};
    print_stats("Mixed Transaction", &stats);
}

// Benchmark: Concurrent read transactions (simulated)
void benchmark_read_transaction(sqlite3 *db, int count) {
    printf("\n=== Benchmarking Read Transaction (%d SELECTs) ===\n", count);
    
    char key[KEY_SIZE];
    
    double start = get_time_us();
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, "SELECT value FROM kv_store WHERE key = ?", -1, &stmt, NULL);
    
    for (int i = 0; i < count; i++) {
        snprintf(key, KEY_SIZE, "batch_key_%08d", rand() % NUM_OPERATIONS);
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    double elapsed = get_time_us() - start;
    
    sqlite3_finalize(stmt);
    
    BenchmarkStats stats = {elapsed, elapsed/count, elapsed/count, elapsed/count, count};
    print_stats("Read Transaction", &stats);
}

// Benchmark: UPSERT operations (INSERT OR REPLACE)
void benchmark_upsert(sqlite3 *db, int count) {
    printf("\n=== Benchmarking %d UPSERT Operations (INSERT OR REPLACE) ===\n", count);
    
    BenchmarkStats stats = {0, 1e9, 0, 0, count};
    sqlite3_stmt *stmt;
    
    const char *sql = "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    char key[KEY_SIZE];
    char value[VALUE_SIZE];
    
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    
    for (int i = 0; i < count; i++) {
        snprintf(key, KEY_SIZE, "batch_key_%08d", rand() % NUM_OPERATIONS);
        generate_random_string(value, VALUE_SIZE);
        
        double start = get_time_us();
        
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, value, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
        
        double elapsed = get_time_us() - start;
        stats.total_time += elapsed;
        if (elapsed < stats.min_time) stats.min_time = elapsed;
        if (elapsed > stats.max_time) stats.max_time = elapsed;
    }
    
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    
    sqlite3_finalize(stmt);
    stats.avg_time = stats.total_time / count;
    print_stats("UPSERT", &stats);
}

// Benchmark: Bulk scan operations
void benchmark_scan(sqlite3 *db) {
    printf("\n=== Benchmarking Full Table SCAN ===\n");
    
    double start = get_time_us();
    
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, "SELECT key, value FROM kv_store", -1, &stmt, NULL);
    
    int count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char *key = sqlite3_column_text(stmt, 0);
        const unsigned char *value = sqlite3_column_text(stmt, 1);
        (void)key; (void)value; // Suppress unused warnings
        count++;
    }
    
    double elapsed = get_time_us() - start;
    sqlite3_finalize(stmt);
    
    printf("  Scanned %d rows\n", count);
    printf("  Total time: %.2f ms\n", elapsed / 1000.0);
    printf("  Average time per row: %.2f µs\n", elapsed / count);
    printf("  Throughput: %.2f rows/sec\n", count * 1000000.0 / elapsed);
    printf("----------------------------------------\n");
}

int main() {
    sqlite3 *db;
    srand(time(NULL));
    
    printf("========================================\n");
    printf("SQLite Key-Value Store Benchmark\n");
    printf("========================================\n");
    
    // Initialize database
    if (init_database(&db, "benchmark.db") != SQLITE_OK) {
        return 1;
    }
    
    // Configure SQLite for better performance
    sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA synchronous=NORMAL", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA cache_size=10000", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA temp_store=MEMORY", NULL, NULL, NULL);
    
    // Run benchmarks
    benchmark_single_insert(db, 1000);
    benchmark_batch_insert(db, NUM_OPERATIONS, BATCH_SIZE);
    benchmark_get(db, NUM_OPERATIONS);
    benchmark_update(db, 5000);
    benchmark_upsert(db, 5000);
    benchmark_delete(db, 5000);
    benchmark_mixed_transaction(db, 5000);
    benchmark_read_transaction(db, 5000);
    benchmark_scan(db);
    
    // Cleanup
    sqlite3_close(db);
    
    printf("\nBenchmark completed!\n");
    printf("Database file: benchmark.db\n");
    
    return 0;
}

