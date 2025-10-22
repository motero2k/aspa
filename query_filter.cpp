#include <bits/stdc++.h>
#include <filesystem>
#include <thread>
#include <mutex>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <fstream>
using namespace std;

// Function to get current RAM usage in bytes
size_t get_current_ram_usage() {
    ifstream stat_file("/proc/meminfo");
    string line;
    size_t mem_active = 0;
    
    while (getline(stat_file, line)) {
        if (line.substr(0, 7) == "Active:") {
            istringstream iss(line);
            string key;
            size_t value_kb;
            string unit;
            iss >> key >> value_kb >> unit;
            mem_active = value_kb * 1024; // Convert to bytes
            break;
        }
    }
    
    return mem_active; // Return active memory
}

struct Accessibility {
    uint32_t origin_id;
    uint32_t destination_id;
    float time;
    float distance;
};

struct AttributeIndex {
    uint32_t block_id;
    uint64_t offset;
    uint32_t count;
};

// Nueva estructura para el índice de destino
struct AccessibilityDestIndex {
    uint32_t destination_id;
    uint32_t block_id;
    uint64_t offset;
    uint32_t count;
};

// New struct for accessibility index
struct AccIndexEntry {
    uint32_t id;        // destination_id
    uint32_t block_id;
    uint64_t offset;
    uint32_t count;
};

// Loads attribute values from block-based structure using mmap
unordered_map<uint32_t, float> load_attribute_values(const string& basePath, uint32_t attr_num) {
    uint32_t attr_index = attr_num - 1;
    string indexPath = basePath + "/index.bin";
    ifstream indexFile(indexPath, ios::binary);
    if (!indexFile) {
        cerr << "Error: Cannot open index file: " << indexPath << endl;
        return {};
    }
    indexFile.seekg(attr_index * sizeof(AttributeIndex));
    AttributeIndex idx;
    if (!indexFile.read((char*)&idx, sizeof(idx))) {
        cerr << "Error: Cannot read index for attribute " << attr_num << endl;
        return {};
    }
    indexFile.close();

    string blockPath = basePath + "/blocks/block_" + to_string(idx.block_id) + ".bin";
    int fd = open(blockPath.c_str(), O_RDONLY);
    if (fd < 0) {
        cerr << "Error: Cannot open block file: " << blockPath << endl;
        return {};
    }
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        cerr << "Error: fstat failed for " << blockPath << endl;
        close(fd);
        return {};
    }
    size_t map_len = sb.st_size;
    void* map = mmap(nullptr, map_len, PROT_READ, MAP_PRIVATE, fd, 0);
    if (map == MAP_FAILED) {
        cerr << "Error: mmap failed for " << blockPath << endl;
        close(fd);
        return {};
    }
    unordered_map<uint32_t, float> values;
    char* ptr = (char*)map + idx.offset;
    for (uint32_t i = 0; i < idx.count; ++i) {
        uint32_t id = *reinterpret_cast<uint32_t*>(ptr + i * 8);
        float val = *reinterpret_cast<float*>(ptr + i * 8 + 4);
        values[id] = val;
    }
    munmap(map, map_len);
    close(fd);
    return values;
}

// Loads accessibility index into memory for fast lookup
unordered_map<uint32_t, AccIndexEntry> load_accessibility_index(const string& basePath) {
    unordered_map<uint32_t, AccIndexEntry> index_map;
    string indexPath = basePath + "/index.bin";
    ifstream indexFile(indexPath, ios::binary);
    AccIndexEntry idx;
    while (indexFile.read(reinterpret_cast<char*>(&idx), sizeof(idx))) {
        index_map[idx.id] = idx;
    }
    indexFile.close();
    return index_map;
}

// Loads accessibility records for a given destination_id using index in memory + mmap
vector<Accessibility> load_accessibility_block(const string& basePath, const AccIndexEntry& idx) {
    string blockPath = basePath + "/blocks/block_" + to_string(idx.block_id) + ".bin";
    int fd = open(blockPath.c_str(), O_RDONLY);
    if (fd < 0) return {};
    struct stat sb;
    if (fstat(fd, &sb) == -1) { close(fd); return {}; }
    size_t map_len = sb.st_size;
    void* map = mmap(nullptr, map_len, PROT_READ, MAP_PRIVATE, fd, 0);
    if (map == MAP_FAILED) { close(fd); return {}; }
    vector<Accessibility> records;
    records.reserve(idx.count);
    char* ptr = (char*)map + idx.offset;
    for (uint32_t i = 0; i < idx.count; ++i) {
        Accessibility a = *reinterpret_cast<Accessibility*>(ptr + i * sizeof(Accessibility));
        records.push_back(a);
    }
    munmap(map, map_len);
    close(fd);
    return records;
}

// Calculate total size of all files in a directory (recursively)
size_t get_directory_size(const string& dirPath) {
    size_t total_size = 0;
    if (filesystem::exists(dirPath) && filesystem::is_directory(dirPath)) {
        for (const auto& entry : filesystem::recursive_directory_iterator(dirPath)) {
            if (entry.is_regular_file()) {
                total_size += entry.file_size();
            }
        }
    }
    return total_size;
}

int main(int argc, char** argv) {
    if (argc < 6) {
        cerr << "Usage: ./query_filter <preprocessed_data_dir> <percent> <origin_attr_name> <dest_attr_name> <results_dir>\n";
        cerr << "Example: ./query_filter out 0.01 or1 dst1 results  (for 1% dataset)\n";
        return 1;
    }

    // === PHASE 0: Program setup ===
    auto t_total_start = chrono::steady_clock::now();
    
    // Start RAM tracking
    size_t ram_min = get_current_ram_usage();
    size_t ram_max = ram_min;
    
    string preprocessedDir = argv[1];
    float percent_float = stof(argv[2]);
    string originAttr = argv[3];
    string destAttr = argv[4];
    string resultsDir = argv[5];
    
    int percent_int = static_cast<int>(percent_float * 100 + 0.5f);
    string percent = to_string(percent_int);
    string suffix = percent + "p";
    
    // Create results directory if it doesn't exist
    filesystem::create_directories(resultsDir);
    
    // Generate output file names with origin and destination attributes
    string outputPath = resultsDir + "/result_" + suffix + "_" + originAttr + "_" + destAttr + ".bin";
    string reportPath = resultsDir + "/result_" + suffix + "_" + originAttr + "_" + destAttr + "_report.txt";
    
    uint32_t originAttrNum = stoi(originAttr.substr(3));
    uint32_t destAttrNum = stoi(destAttr.substr(3));
    
    string preprocessedDataBase = preprocessedDir + "/" + suffix;
    string originBasePath = preprocessedDataBase + "/attributes/origin";
    string destBasePath = preprocessedDataBase + "/attributes/destination";
    string accBasePath = preprocessedDataBase + "/accessibility";
    
    size_t num_threads = thread::hardware_concurrency();
    
    cout << "Dataset: " << percent << "%, Origin: " << originAttr 
         << ", Dest: " << destAttr << ", Threads: " << num_threads << endl;

    // Update RAM tracking
    auto update_ram = [&]() {
        size_t current = get_current_ram_usage();
        ram_min = min(ram_min, current);
        ram_max = max(ram_max, current);
    };

    // === PHASE 1: Load origin attribute index ===
    auto t_phase1_start = chrono::steady_clock::now();
    
    uint32_t attr_index = originAttrNum - 1;
    string originIndexPath = originBasePath + "/index.bin";
    ifstream originIndexFile(originIndexPath, ios::binary);
    if (!originIndexFile) {
        cerr << "Error: Cannot open origin index file" << endl;
        return 1;
    }
    originIndexFile.seekg(attr_index * sizeof(AttributeIndex));
    AttributeIndex originIdx;
    if (!originIndexFile.read((char*)&originIdx, sizeof(originIdx))) {
        cerr << "Error: Cannot read origin index" << endl;
        return 1;
    }
    originIndexFile.close();
    
    auto t_phase1_end = chrono::steady_clock::now();
    double or_idx_load_time = chrono::duration<double>(t_phase1_end - t_phase1_start).count();
    
    update_ram();
    cout << "Phase 1 (load origin index): " << or_idx_load_time << " s" << endl;

    // === PHASE 2: Load origin attribute block ===
    auto t_phase2_start = chrono::steady_clock::now();
    
    string originBlockPath = originBasePath + "/blocks/block_" + to_string(originIdx.block_id) + ".bin";
    int or_fd = open(originBlockPath.c_str(), O_RDONLY);
    if (or_fd < 0) {
        cerr << "Error: Cannot open origin block file" << endl;
        return 1;
    }
    struct stat or_sb;
    fstat(or_fd, &or_sb);
    size_t or_bin_size = or_sb.st_size;
    void* or_map = mmap(nullptr, or_bin_size, PROT_READ, MAP_PRIVATE, or_fd, 0);
    
    unordered_map<uint32_t, float> originValues;
    char* or_ptr = (char*)or_map + originIdx.offset;
    for (uint32_t i = 0; i < originIdx.count; ++i) {
        uint32_t id = *reinterpret_cast<uint32_t*>(or_ptr + i * 8);
        float val = *reinterpret_cast<float*>(or_ptr + i * 8 + 4);
        originValues[id] = val;
    }
    munmap(or_map, or_bin_size);
    close(or_fd);
    
    // Get total size of origin directory (includes blocks and index)
    size_t or_blocks_total_size = get_directory_size(originBasePath);
    
    auto t_phase2_end = chrono::steady_clock::now();
    double or_bin_load_time = chrono::duration<double>(t_phase2_end - t_phase2_start).count();
    size_t or_bin_loaded_rows = originValues.size();
    size_t or_bin_loaded_size = or_bin_loaded_rows * 8;
    
    update_ram();
    cout << "Phase 2 (load origin attributes): " << or_bin_load_time << " s" << endl;
    cout << "  Origin loaded rows: " << or_bin_loaded_rows << endl;
    cout << "  Origin block file size: " << or_bin_size << " bytes" << endl;
    cout << "  Origin directory total on disk: " << or_blocks_total_size << " bytes" << endl;

    // === PHASE 3: Load destination attribute index ===
    auto t_phase3_start = chrono::steady_clock::now();
    
    uint32_t dest_attr_index = destAttrNum - 1;
    string destIndexPath = destBasePath + "/index.bin";
    ifstream destIndexFile(destIndexPath, ios::binary);
    if (!destIndexFile) {
        cerr << "Error: Cannot open dest index file" << endl;
        return 1;
    }
    destIndexFile.seekg(dest_attr_index * sizeof(AttributeIndex));
    AttributeIndex destIdx;
    if (!destIndexFile.read((char*)&destIdx, sizeof(destIdx))) {
        cerr << "Error: Cannot read dest index" << endl;
        return 1;
    }
    destIndexFile.close();
    
    auto t_phase3_end = chrono::steady_clock::now();
    double dst_idx_load_time = chrono::duration<double>(t_phase3_end - t_phase3_start).count();
    
    update_ram();
    cout << "Phase 3 (load dest index): " << dst_idx_load_time << " s" << endl;

    // === PHASE 4: Load destination attribute block ===
    auto t_phase4_start = chrono::steady_clock::now();
    
    string destBlockPath = destBasePath + "/blocks/block_" + to_string(destIdx.block_id) + ".bin";
    int dst_fd = open(destBlockPath.c_str(), O_RDONLY);
    if (dst_fd < 0) {
        cerr << "Error: Cannot open dest block file" << endl;
        return 1;
    }
    struct stat dst_sb;
    fstat(dst_fd, &dst_sb);
    size_t dst_bin_size = dst_sb.st_size;
    void* dst_map = mmap(nullptr, dst_bin_size, PROT_READ, MAP_PRIVATE, dst_fd, 0);
    
    unordered_map<uint32_t, float> destValues;
    char* dst_ptr = (char*)dst_map + destIdx.offset;
    for (uint32_t i = 0; i < destIdx.count; ++i) {
        uint32_t id = *reinterpret_cast<uint32_t*>(dst_ptr + i * 8);
        float val = *reinterpret_cast<float*>(dst_ptr + i * 8 + 4);
        destValues[id] = val;
    }
    munmap(dst_map, dst_bin_size);
    close(dst_fd);
    
    // Get total size of destination directory (includes blocks and index)
    size_t dst_blocks_total_size = get_directory_size(destBasePath);
    
    auto t_phase4_end = chrono::steady_clock::now();
    double dst_bin_load_time = chrono::duration<double>(t_phase4_end - t_phase4_start).count();
    size_t dst_bin_loaded_rows = destValues.size();
    size_t dst_bin_loaded_size = dst_bin_loaded_rows * 8;
    
    update_ram();
    cout << "Phase 4 (load dest attributes): " << dst_bin_load_time << " s" << endl;
    cout << "  Destination loaded rows: " << dst_bin_loaded_rows << endl;
    cout << "  Destination block file size: " << dst_bin_size << " bytes" << endl;
    cout << "  Destination directory total on disk: " << dst_blocks_total_size << " bytes" << endl;

    // === PHASE 5: Load accessibility index ===
    auto t_phase5_start = chrono::steady_clock::now();
    
    unordered_map<uint32_t, AccIndexEntry> accIndexMap;
    string accIndexPath = accBasePath + "/index.bin";
    ifstream accIndexFile(accIndexPath, ios::binary);
    AccIndexEntry idx;
    while (accIndexFile.read(reinterpret_cast<char*>(&idx), sizeof(idx))) {
        accIndexMap[idx.id] = idx;
    }
    accIndexFile.close();
    
    vector<uint32_t> selected_dest_ids;
    for (const auto& [dest_id, _] : destValues) {
        selected_dest_ids.push_back(dest_id);
    }
    
    auto t_phase5_end = chrono::steady_clock::now();
    double acc_idx_load_time = chrono::duration<double>(t_phase5_end - t_phase5_start).count();
    
    update_ram();
    cout << "Phase 5 (load accessibility index): " << acc_idx_load_time << " s" << endl;
    cout << "  Selected destinations: " << selected_dest_ids.size() << endl;

    // === PHASE 6: Load accessibility blocks (data) ===
    auto t_phase6_start = chrono::steady_clock::now();
    
    unordered_map<uint32_t, vector<Accessibility>> loaded_acc_data;
    size_t acc_bin_loaded_rows = 0;
    
    for (uint32_t dest_id : selected_dest_ids) {
        auto it = accIndexMap.find(dest_id);
        if (it == accIndexMap.end()) continue;
        auto records = load_accessibility_block(accBasePath, it->second);
        acc_bin_loaded_rows += records.size();
        loaded_acc_data[dest_id] = move(records);
    }
    
    // Get total size of accessibility directory (includes blocks and index)
    size_t acc_blocks_total_size = get_directory_size(accBasePath);
    
    auto t_phase6_end = chrono::steady_clock::now();
    double acc_bin_load_time = chrono::duration<double>(t_phase6_end - t_phase6_start).count();
    
    update_ram();
    cout << "Phase 6 (load accessibility blocks): " << acc_bin_load_time << " s" << endl;
    cout << "  Accessibility loaded rows: " << acc_bin_loaded_rows << endl;
    cout << "  Accessibility loaded size: " << (acc_bin_loaded_rows * sizeof(Accessibility)) << " bytes" << endl;
    cout << "  Accessibility directory total on disk: " << acc_blocks_total_size << " bytes" << endl;

    // === PHASE 7: Filtering (in-memory) ===
    auto t_phase7_start = chrono::steady_clock::now();
    
    vector<thread> threads;
    mutex results_mutex;
    vector<Accessibility> filtered_results;
    size_t result_acc_rows = 0;
    
    auto process_dest_range = [&](size_t start, size_t end) {
        vector<Accessibility> local_results;
        
        for (size_t i = start; i < end; ++i) {
            uint32_t dest_id = selected_dest_ids[i];
            auto data_it = loaded_acc_data.find(dest_id);
            if (data_it == loaded_acc_data.end()) continue;
            
            for (const auto& a : data_it->second) {
                auto originIt = originValues.find(a.origin_id);
                if (originIt != originValues.end()) {
                    local_results.push_back(a);
                }
            }
        }
        
        lock_guard<mutex> lock(results_mutex);
        filtered_results.insert(filtered_results.end(), local_results.begin(), local_results.end());
    };
    
    size_t total = selected_dest_ids.size();
    size_t chunk = (total + num_threads - 1) / num_threads;
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * chunk;
        size_t end = min(start + chunk, total);
        if (start >= end) break;
        threads.emplace_back(process_dest_range, start, end);
    }
    for (auto& th : threads) th.join();
    
    result_acc_rows = filtered_results.size();
    
    auto t_phase7_end = chrono::steady_clock::now();
    double time_filtering = chrono::duration<double>(t_phase7_end - t_phase7_start).count();
    size_t result_acc_size = result_acc_rows * sizeof(Accessibility);
    
    update_ram();
    cout << "Phase 7 (filtering): " << time_filtering << " s" << endl;
    cout << "  Result rows: " << result_acc_rows << endl;
    cout << "  Result size: " << result_acc_size << " bytes" << endl;

    // === PHASE 8: Write results as binary ===
    auto t_phase8_start = chrono::steady_clock::now();
    
    ofstream fout(outputPath, ios::binary);
    fout.write(reinterpret_cast<const char*>(filtered_results.data()), 
               filtered_results.size() * sizeof(Accessibility));
    fout.close();
    
    auto t_phase8_end = chrono::steady_clock::now();
    double time_write_bin = chrono::duration<double>(t_phase8_end - t_phase8_start).count();
    
    update_ram();
    cout << "Phase 8 (write binary results): " << time_write_bin << " s" << endl;
    
    // === Generate result report ===
    ofstream report(reportPath);
    report << "========================================\n";
    report << "QUERY RESULT REPORT\n";
    report << "========================================\n\n";
    report << "Binary file: " << outputPath << "\n";
    report << "Dataset percentage: " << percent << "%\n";
    report << "Origin attribute: " << originAttr << " (attr #" << originAttrNum << ")\n";
    report << "Destination attribute: " << destAttr << " (attr #" << destAttrNum << ")\n\n";
    
    report << "========================================\n";
    report << "BINARY FORMAT DESCRIPTION\n";
    report << "========================================\n";
    report << "Structure: Accessibility table (filtered)\n";
    report << "Record size: " << sizeof(Accessibility) << " bytes\n";
    report << "Total records: " << result_acc_rows << "\n";
    report << "Total size: " << result_acc_size << " bytes\n\n";
    
    report << "Field layout per record:\n";
    report << "  Offset 0-3   (4 bytes): origin_id (uint32_t)\n";
    report << "  Offset 4-7   (4 bytes): destination_id (uint32_t)\n";
    report << "  Offset 8-11  (4 bytes): time (float)\n";
    report << "  Offset 12-15 (4 bytes): distance (float)\n\n";
    
    report << "========================================\n";
    report << "PERFORMANCE SUMMARY\n";
    report << "========================================\n";
    report << "Total time: " << (chrono::duration<double>(t_phase8_end - t_total_start).count()) << " s\n";
    report << "  - Load origin index: " << or_idx_load_time << " s\n";
    report << "  - Load origin data: " << or_bin_load_time << " s\n";
    report << "  - Load dest index: " << dst_idx_load_time << " s\n";
    report << "  - Load dest data: " << dst_bin_load_time << " s\n";
    report << "  - Load accessibility index: " << acc_idx_load_time << " s\n";
    report << "  - Load accessibility data: " << acc_bin_load_time << " s\n";
    report << "  - Filtering: " << time_filtering << " s\n";
    report << "  - Write binary: " << time_write_bin << " s\n\n";
    
    report << "========================================\n";
    report << "DATA STATISTICS\n";
    report << "========================================\n";
    report << "Origin attributes loaded: " << or_bin_loaded_rows << " rows\n";
    report << "Destination attributes loaded: " << dst_bin_loaded_rows << " rows\n";
    report << "Accessibility records loaded: " << acc_bin_loaded_rows << " rows\n";
    report << "Result records: " << result_acc_rows << " rows\n";
    report << "Selectivity: " << fixed << setprecision(2) 
           << (100.0 * result_acc_rows / acc_bin_loaded_rows) << "%\n";
    
    report << "\n========================================\n";
    report << "RAM USAGE STATISTICS\n";
    report << "========================================\n";
    report << "RAM Min: " << ram_min << " bytes\n";
    report << "RAM Max: " << ram_max << " bytes\n";
    report << "RAM Max-Min: " << (ram_max - ram_min) << " bytes\n";
    report << "========================================\n";
    
    report.close();

    // === PHASE 9: Total summary ===
    auto t_total_end = chrono::steady_clock::now();
    double total_time = chrono::duration<double>(t_total_end - t_total_start).count();
    
    cout << "\n=== FINAL SUMMARY ===" << endl;
    cout << "Total time: " << total_time << " s" << endl;
    cout << "RAM usage - Min: " << ram_min << " bytes, Max: " << ram_max 
         << " bytes, Max-Min: " << (ram_max - ram_min) << " bytes" << endl;
    cout << "Binary result: " << outputPath << endl;
    cout << "Report: " << reportPath << endl;
    
    // Write to CSV file
    string csvPath = "report.csv";
    
    bool file_exists = filesystem::exists(csvPath);
    ofstream csvOut(csvPath, ios::app);
    
    // Write header only if file doesn't exist
    if (!file_exists) {
        csvOut << "dataset_%,origin_attr_%,dest_attr_%,"
               << "or_bin_loaded_rows,dst_bin_loaded_rows,acc_bin_loaded_rows,"
               << "or_bin_loaded_size (B),dst_bin_loaded_size (B),acc_bin_loaded_size (B),"
               << "or_bin_load_time (s),dst_bin_load_time (s),acc_bin_load_time (s),"
               << "time_filtering (s),time_write_bin (s),total_time (s),result_acc_size (B),"
               << "num_threads,RAM_min (B),RAM_max (B),RAM_max-min (B)\n";
    }
    
    size_t acc_bin_loaded_size_bytes = acc_bin_loaded_rows * sizeof(Accessibility);
    
    // Write data row
    csvOut << percent << "," << originAttr << "," << destAttr << ","
           << or_bin_loaded_rows << "," << dst_bin_loaded_rows << "," << acc_bin_loaded_rows << ","
           << or_bin_loaded_size << "," << dst_bin_loaded_size << "," << acc_bin_loaded_size_bytes << ","
           << or_bin_load_time << "," << dst_bin_load_time << "," << acc_bin_load_time << ","
           << time_filtering << "," << time_write_bin << "," << total_time << ","
           << result_acc_size << ","
           << num_threads << "," << ram_min << "," << ram_max << "," << (ram_max - ram_min) << "\n";
    
    csvOut.close();
    
    cout << "Results appended to: " << csvPath << endl;
    cout << "\nQuery done!" << endl;
}
