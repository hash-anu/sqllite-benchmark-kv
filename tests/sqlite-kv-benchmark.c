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

// Generate random binary data
void generate_random_blob(unsigned char *blob, size_t length) {
    for (size_t i = 0; i < length; i++) {
        blob[i] = rand() % 256;
    }
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

// Initialize database with BLOB columns
int init_database(sqlite3 **db, const char *db_path) {
    int rc = sqlite3_open(db_path, db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(*db));
        return rc;
    }
    
    printf("SQLite version: %s\n", sqlite3_libversion());
    
    // Create table with BLOB key and BLOB value
    const char *create_table = 
        "CREATE TABLE IF NOT EXISTS kv_store ("
        "key BLOB PRIMARY KEY, "
        "value BLOB NOT NULL)";
    
    char *err_msg = NULL;
    rc = sqlite3_exec(*db, create_table, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return rc;
    }
    
    // Note: Index on BLOB is automatically created for PRIMARY KEY
    printf("Table created with BLOB key and BLOB value\n");
    
    return rc;
}

// Benchmark: Single INSERT operations (auto-commit)
void benchmark_single_insert(sqlite3 *db, int count) {
    printf("\n=== Benchmarking %d Individual INSERTs (Auto-commit) ===\n", count);
    
    BenchmarkStats stats = {0, 1e9, 0, 0, count};
    sqlite3_stmt *stmt;
    
    const char *sql = "INSERT INTO kv_store (key, value) VALUES (?, ?)";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    unsigned char *key = malloc(KEY_SIZE);
    unsigned char *value = malloc(VALUE_SIZE);
    
    for (int i = 0; i < count; i++) {
        generate_random_blob(key, KEY_SIZE);
        generate_random_blob(value, VALUE_SIZE);
        
        double start = get_time_us();
        
        sqlite3_bind_blob(stmt, 1, key, KEY_SIZE, SQLITE_TRANSIENT);
        sqlite3_bind_blob(stmt, 2, value, VALUE_SIZE, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
        
        double elapsed = get_time_us() - start;
        stats.total_time += elapsed;
        if (elapsed < stats.min_time) stats.min_time = elapsed;
        if (elapsed > stats.max_time) stats.max_time = elapsed;
    }
    
    free(key);
    free(value);
    sqlite3_finalize(stmt);
    stats.avg_time = stats.total_time / count;
    print_stats("Single INSERT (BLOB)", &stats);
}

// Benchmark: Batch INSERT with transaction
void benchmark_batch_insert(sqlite3 *db, int count, int batch_size) {
    printf("\n=== Benchmarking Batch INSERT (%d records, batch size %d) ===\n", 
           count, batch_size);
    
    BenchmarkStats stats = {0, 0, 0, 0, 1};
    sqlite3_stmt *stmt;
    
    const char *sql = "INSERT INTO kv_store (key, value) VALUES (?, ?)";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    unsigned char *key = malloc(KEY_SIZE);
    unsigned char *value = malloc(VALUE_SIZE);
    
    double start = get_time_us();
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    
    for (int i = 0; i < count; i++) {
        generate_random_blob(key, KEY_SIZE);
        generate_random_blob(value, VALUE_SIZE);
        
        sqlite3_bind_blob(stmt, 1, key, KEY_SIZE, SQLITE_TRANSIENT);
        sqlite3_bind_blob(stmt, 2, value, VALUE_SIZE, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
        
        if ((i + 1) % batch_size == 0) {
            sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
            sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
        }
    }
    
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    double elapsed = get_time_us() - start;
    
    free(key);
    free(value);
    sqlite3_finalize(stmt);
    
    stats.total_time = elapsed;
    stats.avg_time = elapsed / count;
    stats.min_time = stats.avg_time;
    stats.max_time = stats.avg_time;
    stats.count = count;
    
    print_stats("Batch INSERT (BLOB)", &stats);
}

// Store keys for later retrieval
unsigned char **stored_keys = NULL;
int stored_keys_count = 0;

// Modified batch insert that stores keys for later use
void benchmark_batch_insert_with_keys(sqlite3 *db, int count, int batch_size) {
    printf("\n=== Benchmarking Batch INSERT with Key Storage (%d records) ===\n", count);
    
    BenchmarkStats stats = {0, 0, 0, 0, 1};
    sqlite3_stmt *stmt;
    
    const char *sql = "INSERT INTO kv_store (key, value) VALUES (?, ?)";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    // Allocate memory to store keys for later retrieval
    stored_keys = malloc(count * sizeof(unsigned char*));
    stored_keys_count = count;
    
    unsigned char *value = malloc(VALUE_SIZE);
    
    double start = get_time_us();
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    
    for (int i = 0; i < count; i++) {
        stored_keys[i] = malloc(KEY_SIZE);
        generate_random_blob(stored_keys[i], KEY_SIZE);
        generate_random_blob(value, VALUE_SIZE);
        
        sqlite3_bind_blob(stmt, 1, stored_keys[i], KEY_SIZE, SQLITE_TRANSIENT);
        sqlite3_bind_blob(stmt, 2, value, VALUE_SIZE, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
        
        if ((i + 1) % batch_size == 0) {
            sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
            sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
        }
    }
    
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    double elapsed = get_time_us() - start;
    
    free(value);
    sqlite3_finalize(stmt);
    
    stats.total_time = elapsed;
    stats.avg_time = elapsed / count;
    stats.min_time = stats.avg_time;
    stats.max_time = stats.avg_time;
    stats.count = count;
    
    print_stats("Batch INSERT with Keys (BLOB)", &stats);
}

// Benchmark: GET operations using BLOB keys
void benchmark_get(sqlite3 *db, int count) {
    printf("\n=== Benchmarking %d GET Operations (BLOB keys) ===\n", count);
    
    if (stored_keys == NULL || stored_keys_count == 0) {
        printf("No keys available for GET benchmark. Run batch insert first.\n");
        return;
    }
    
    BenchmarkStats stats = {0, 1e9, 0, 0, count};
    sqlite3_stmt *stmt;
    
    const char *sql = "SELECT value FROM kv_store WHERE key = ?";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    for (int i = 0; i < count; i++) {
        int key_idx = rand() % stored_keys_count;
        
        double start = get_time_us();
        
        sqlite3_bind_blob(stmt, 1, stored_keys[key_idx], KEY_SIZE, SQLITE_TRANSIENT);
        int rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW) {
            // Read the BLOB value
            const void *blob = sqlite3_column_blob(stmt, 0);
            int blob_size = sqlite3_column_bytes(stmt, 0);
            (void)blob; (void)blob_size; // Suppress unused warning
        }
        sqlite3_reset(stmt);
        
        double elapsed = get_time_us() - start;
        stats.total_time += elapsed;
        if (elapsed < stats.min_time) stats.min_time = elapsed;
        if (elapsed > stats.max_time) stats.max_time = elapsed;
    }
    
    sqlite3_finalize(stmt);
    stats.avg_time = stats.total_time / count;
    print_stats("GET (BLOB)", &stats);
}

// Benchmark: UPDATE operations with BLOB
void benchmark_update(sqlite3 *db, int count) {
    printf("\n=== Benchmarking %d UPDATE Operations (BLOB) ===\n", count);
    
    if (stored_keys == NULL || stored_keys_count == 0) {
        printf("No keys available for UPDATE benchmark.\n");
        return;
    }
    
    BenchmarkStats stats = {0, 1e9, 0, 0, count};
    sqlite3_stmt *stmt;
    
    const char *sql = "UPDATE kv_store SET value = ? WHERE key = ?";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    unsigned char *value = malloc(VALUE_SIZE);
    
    for (int i = 0; i < count; i++) {
        int key_idx = rand() % stored_keys_count;
        generate_random_blob(value, VALUE_SIZE);
        
        double start = get_time_us();
        
        sqlite3_bind_blob(stmt, 1, value, VALUE_SIZE, SQLITE_TRANSIENT);
        sqlite3_bind_blob(stmt, 2, stored_keys[key_idx], KEY_SIZE, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
        
        double elapsed = get_time_us() - start;
        stats.total_time += elapsed;
        if (elapsed < stats.min_time) stats.min_time = elapsed;
        if (elapsed > stats.max_time) stats.max_time = elapsed;
    }
    
    free(value);
    sqlite3_finalize(stmt);
    stats.avg_time = stats.total_time / count;
    print_stats("UPDATE (BLOB)", &stats);
}

// Benchmark: DELETE operations with BLOB keys
void benchmark_delete(sqlite3 *db, int count) {
    printf("\n=== Benchmarking %d DELETE Operations (BLOB keys) ===\n", count);
    
    if (stored_keys == NULL || stored_keys_count == 0) {
        printf("No keys available for DELETE benchmark.\n");
        return;
    }
    
    BenchmarkStats stats = {0, 1e9, 0, 0, count};
    sqlite3_stmt *stmt;
    
    const char *sql = "DELETE FROM kv_store WHERE key = ?";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    int delete_count = (count > stored_keys_count) ? stored_keys_count : count;
    
    for (int i = 0; i < delete_count; i++) {
        double start = get_time_us();
        
        sqlite3_bind_blob(stmt, 1, stored_keys[i], KEY_SIZE, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
        
        double elapsed = get_time_us() - start;
        stats.total_time += elapsed;
        if (elapsed < stats.min_time) stats.min_time = elapsed;
        if (elapsed > stats.max_time) stats.max_time = elapsed;
    }
    
    sqlite3_finalize(stmt);
    stats.count = delete_count;
    stats.avg_time = stats.total_time / delete_count;
    print_stats("DELETE (BLOB)", &stats);
}

// Benchmark: UPSERT operations (INSERT OR REPLACE) with BLOB
void benchmark_upsert(sqlite3 *db, int count) {
    printf("\n=== Benchmarking %d UPSERT Operations (BLOB) ===\n", count);
    
    if (stored_keys == NULL || stored_keys_count == 0) {
        printf("No keys available for UPSERT benchmark.\n");
        return;
    }
    
    BenchmarkStats stats = {0, 1e9, 0, 0, count};
    sqlite3_stmt *stmt;
    
    const char *sql = "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)";
    sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    
    unsigned char *value = malloc(VALUE_SIZE);
    
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    
    for (int i = 0; i < count; i++) {
        int key_idx = rand() % stored_keys_count;
        generate_random_blob(value, VALUE_SIZE);
        
        double start = get_time_us();
        
        sqlite3_bind_blob(stmt, 1, stored_keys[key_idx], KEY_SIZE, SQLITE_TRANSIENT);
        sqlite3_bind_blob(stmt, 2, value, VALUE_SIZE, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
        
        double elapsed = get_time_us() - start;
        stats.total_time += elapsed;
        if (elapsed < stats.min_time) stats.min_time = elapsed;
        if (elapsed > stats.max_time) stats.max_time = elapsed;
    }
    
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    
    free(value);
    sqlite3_finalize(stmt);
    stats.avg_time = stats.total_time / count;
    print_stats("UPSERT (BLOB)", &stats);
}

// Benchmark: Mixed transaction with BLOB
void benchmark_mixed_transaction(sqlite3 *db, int count) {
    printf("\n=== Benchmarking Mixed Transaction (BLOB, %d ops) ===\n", count);
    
    if (stored_keys == NULL || stored_keys_count == 0) {
        printf("No keys available for mixed transaction benchmark.\n");
        return;
    }
    
    unsigned char *key = malloc(KEY_SIZE);
    unsigned char *value = malloc(VALUE_SIZE);
    
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
        int key_idx = rand() % stored_keys_count;
        generate_random_blob(value, VALUE_SIZE);
        
        switch(op) {
            case 0: // INSERT with new random key
                generate_random_blob(key, KEY_SIZE);
                sqlite3_bind_blob(insert_stmt, 1, key, KEY_SIZE, SQLITE_TRANSIENT);
                sqlite3_bind_blob(insert_stmt, 2, value, VALUE_SIZE, SQLITE_TRANSIENT);
                sqlite3_step(insert_stmt);
                sqlite3_reset(insert_stmt);
                break;
            case 1: // UPDATE existing key
                sqlite3_bind_blob(update_stmt, 1, value, VALUE_SIZE, SQLITE_TRANSIENT);
                sqlite3_bind_blob(update_stmt, 2, stored_keys[key_idx], KEY_SIZE, SQLITE_TRANSIENT);
                sqlite3_step(update_stmt);
                sqlite3_reset(update_stmt);
                break;
            case 2: // SELECT
                sqlite3_bind_blob(select_stmt, 1, stored_keys[key_idx], KEY_SIZE, SQLITE_TRANSIENT);
                sqlite3_step(select_stmt);
                sqlite3_reset(select_stmt);
                break;
        }
    }
    
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    double elapsed = get_time_us() - start;
    
    free(key);
    free(value);
    sqlite3_finalize(insert_stmt);
    sqlite3_finalize(update_stmt);
    sqlite3_finalize(select_stmt);
    
    BenchmarkStats stats = {elapsed, elapsed/count, elapsed/count, elapsed/count, count};
    print_stats("Mixed Transaction (BLOB)", &stats);
}

// Benchmark: Read transaction with BLOB
void benchmark_read_transaction(sqlite3 *db, int count) {
    printf("\n=== Benchmarking Read Transaction (BLOB, %d SELECTs) ===\n", count);
    
    if (stored_keys == NULL || stored_keys_count == 0) {
        printf("No keys available for read transaction benchmark.\n");
        return;
    }
    
    double start = get_time_us();
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, "SELECT value FROM kv_store WHERE key = ?", -1, &stmt, NULL);
    
    for (int i = 0; i < count; i++) {
        int key_idx = rand() % stored_keys_count;
        sqlite3_bind_blob(stmt, 1, stored_keys[key_idx], KEY_SIZE, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    double elapsed = get_time_us() - start;
    
    sqlite3_finalize(stmt);
    
    BenchmarkStats stats = {elapsed, elapsed/count, elapsed/count, elapsed/count, count};
    print_stats("Read Transaction (BLOB)", &stats);
}

// Benchmark: Full table scan with BLOB
void benchmark_scan(sqlite3 *db) {
    printf("\n=== Benchmarking Full Table SCAN (BLOB) ===\n");
    
    double start = get_time_us();
    
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, "SELECT key, value FROM kv_store", -1, &stmt, NULL);
    
    int count = 0;
    size_t total_key_bytes = 0;
    size_t total_value_bytes = 0;
    
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const void *key = sqlite3_column_blob(stmt, 0);
        int key_size = sqlite3_column_bytes(stmt, 0);
        const void *value = sqlite3_column_blob(stmt, 1);
        int value_size = sqlite3_column_bytes(stmt, 1);
        
        (void)key; (void)value; // Suppress unused warnings
        total_key_bytes += key_size;
        total_value_bytes += value_size;
        count++;
    }
    
    double elapsed = get_time_us() - start;
    sqlite3_finalize(stmt);
    
    printf("  Scanned %d rows\n", count);
    printf("  Total key bytes: %zu (avg: %.1f bytes)\n", total_key_bytes, (double)total_key_bytes/count);
    printf("  Total value bytes: %zu (avg: %.1f bytes)\n", total_value_bytes, (double)total_value_bytes/count);
    printf("  Total time: %.2f ms\n", elapsed / 1000.0);
    printf("  Average time per row: %.2f µs\n", elapsed / count);
    printf("  Throughput: %.2f rows/sec\n", count * 1000000.0 / elapsed);
    printf("----------------------------------------\n");
}

// Benchmark: Variable size BLOBs
void benchmark_variable_blob_sizes(sqlite3 *db) {
    printf("\n=== Benchmarking Variable BLOB Sizes ===\n");
    
    int sizes[] = {16, 64, 256, 1024, 4096, 16384, 65536};
    int num_sizes = sizeof(sizes) / sizeof(sizes[0]);
    int ops_per_size = 1000;
    
    sqlite3_stmt *insert_stmt, *select_stmt;
    sqlite3_prepare_v2(db, "INSERT INTO kv_store (key, value) VALUES (?, ?)", 
                       -1, &insert_stmt, NULL);
    sqlite3_prepare_v2(db, "SELECT value FROM kv_store WHERE key = ?", 
                       -1, &select_stmt, NULL);
    
    for (int s = 0; s < num_sizes; s++) {
        int size = sizes[s];
        unsigned char *key = malloc(KEY_SIZE);
        unsigned char *value = malloc(size);
        
        // Insert benchmark
        double insert_start = get_time_us();
        sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
        
        for (int i = 0; i < ops_per_size; i++) {
            generate_random_blob(key, KEY_SIZE);
            generate_random_blob(value, size);
            sqlite3_bind_blob(insert_stmt, 1, key, KEY_SIZE, SQLITE_TRANSIENT);
            sqlite3_bind_blob(insert_stmt, 2, value, size, SQLITE_TRANSIENT);
            sqlite3_step(insert_stmt);
            sqlite3_reset(insert_stmt);
        }
        
        sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
        double insert_elapsed = get_time_us() - insert_start;
        
        printf("  Size %d bytes: INSERT %.2f µs/op (%.2f MB/s)\n", 
               size, 
               insert_elapsed / ops_per_size,
               (size * ops_per_size) / insert_elapsed);
        
        free(key);
        free(value);
        
        // Clean up for next iteration
        sqlite3_exec(db, "DELETE FROM kv_store", NULL, NULL, NULL);
    }
    
    sqlite3_finalize(insert_stmt);
    sqlite3_finalize(select_stmt);
    printf("----------------------------------------\n");
}

// Cleanup stored keys
void cleanup_stored_keys() {
    if (stored_keys != NULL) {
        for (int i = 0; i < stored_keys_count; i++) {
            free(stored_keys[i]);
        }
        free(stored_keys);
        stored_keys = NULL;
        stored_keys_count = 0;
    }
}

int main() {
    sqlite3 *db;
    srand(time(NULL));
    
    printf("========================================\n");
    printf("SQLite BLOB Key-Value Store Benchmark\n");
    printf("Key: BLOB (%d bytes), Value: BLOB (%d bytes)\n", KEY_SIZE, VALUE_SIZE);
    printf("========================================\n");
    
    // Initialize database
    if (init_database(&db, "benchmark_blob.db") != SQLITE_OK) {
        return 1;
    }
    
    // Configure SQLite for better performance
    sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA synchronous=NORMAL", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA cache_size=10000", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA temp_store=MEMORY", NULL, NULL, NULL);
    
    // Run benchmarks
    benchmark_single_insert(db, 1000);
    benchmark_batch_insert_with_keys(db, NUM_OPERATIONS, BATCH_SIZE);
    benchmark_get(db, NUM_OPERATIONS);
    benchmark_update(db, 5000);
    benchmark_upsert(db, 5000);
    benchmark_delete(db, 5000);
    benchmark_mixed_transaction(db, 5000);
    benchmark_read_transaction(db, 5000);
    benchmark_scan(db);
    
    // Clean up and test variable sizes
    sqlite3_exec(db, "DELETE FROM kv_store", NULL, NULL, NULL);
    benchmark_variable_blob_sizes(db);
    
    // Cleanup
    cleanup_stored_keys();
    sqlite3_close(db);
    
    printf("\nBenchmark completed!\n");
    printf("Database file: benchmark_blob.db\n");
    
    return 0;
}

/* 
 * Compilation instructions:
 * gcc -o sqlite_blob_benchmark sqlite_blob_benchmark.c -lsqlite3 -O3
 * 
 * Run:
 * ./sqlite_blob_benchmark
 */
