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

// ============================================================
// QUERY FILTER - MULTI-ATTRIBUTE VERSION
// ============================================================
// Accepts multiple origin and destination attributes
// Usage: ./query_filter_multi <data_dir> <percent> <origin_attrs> <dest_attrs> <results_dir>
// Example: ./query_filter_multi dataset_processed 0.01 "1,5,10" "25,30" results
// ============================================================

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
    
    return mem_active;
}

// Parse comma-separated attribute list: "1,5,10" → [1, 5, 10]
vector<uint32_t> parse_attribute_list(const string& attr_string) {
    vector<uint32_t> result;
    stringstream ss(attr_string);
    string item;
    
    while (getline(ss, item, ',')) {
        // Remove whitespace
        item.erase(remove(item.begin(), item.end(), ' '), item.end());
        // Handle "attN" format or plain numbers
        if (item.substr(0, 3) == "att") {
            result.push_back(stoi(item.substr(3)));
        } else {
            result.push_back(stoi(item));
        }
    }
    
    return result;
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

struct AccIndexEntry {
    uint32_t id;
    uint32_t block_id;
    uint64_t offset;
    uint32_t count;
};

// Thread-safe logging
mutex cout_mutex;
void log_msg(const string& msg) {
    lock_guard<mutex> lock(cout_mutex);
    cout << msg << flush;
}

// Load attribute values using mmap
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

// Load accessibility index
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

// Load accessibility block
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
        cerr << "Usage: ./query_filter_multi <preprocessed_data_dir> <percent> <origin_attrs> <dest_attrs> <results_dir>\n";
        cerr << "Example: ./query_filter_multi dataset_processed 0.01 \"1,5,10\" \"25,30\" results\n";
        cerr << "- origin_attrs: comma-separated attribute numbers (e.g., \"1,5,10\")\n";
        cerr << "- dest_attrs: comma-separated attribute numbers (e.g., \"25,30\")\n";
        return 1;
    }

    // === PHASE 0: Program setup ===
    auto t_total_start = chrono::steady_clock::now();
    
    size_t ram_min = get_current_ram_usage();
    size_t ram_max = ram_min;
    
    string preprocessedDir = argv[1];
    float percent_float = stof(argv[2]);
    string originAttrsStr = argv[3];
    string destAttrsStr = argv[4];
    string resultsDir = argv[5];
    
    // Parse attribute lists
    vector<uint32_t> originAttrNums = parse_attribute_list(originAttrsStr);
    vector<uint32_t> destAttrNums = parse_attribute_list(destAttrsStr);
    
    int percent_int = static_cast<int>(percent_float * 100 + 0.5f);
    string percent = to_string(percent_int);
    string suffix = percent + "p";
    
    filesystem::create_directories(resultsDir);
    
    // Generate output file names
    string originStr = "", destStr = "";
    for (size_t i = 0; i < originAttrNums.size(); i++) {
        if (i > 0) originStr += "_";
        originStr += "a" + to_string(originAttrNums[i]);
    }
    for (size_t i = 0; i < destAttrNums.size(); i++) {
        if (i > 0) destStr += "_";
        destStr += "a" + to_string(destAttrNums[i]);
    }
    
    string outputPath = resultsDir + "/result_" + suffix + "_or_" + originStr + "_dst_" + destStr + ".bin";
    string reportPath = resultsDir + "/result_" + suffix + "_or_" + originStr + "_dst_" + destStr + "_report.txt";
    
    string preprocessedDataBase = preprocessedDir + "/" + suffix;
    string originBasePath = preprocessedDataBase + "/attributes/origin";
    string destBasePath = preprocessedDataBase + "/attributes/destination";
    string accBasePath = preprocessedDataBase + "/accessibility";
    
    size_t num_threads = thread::hardware_concurrency();
    
    log_msg("Dataset: " + percent + "%, Origin attrs: " + originAttrsStr + 
            ", Dest attrs: " + destAttrsStr + ", Threads: " + to_string(num_threads) + "\n");

    auto update_ram = [&]() {
        size_t current = get_current_ram_usage();
        ram_min = min(ram_min, current);
        ram_max = max(ram_max, current);
    };

    // === PHASE 1-2: Load ALL origin attributes ===
    auto t_phase12_start = chrono::steady_clock::now();
    
    vector<unordered_map<uint32_t, float>> originMaps;
    size_t or_total_loaded_rows = 0;
    
    for (uint32_t attr : originAttrNums) {
        auto values = load_attribute_values(originBasePath, attr);
        originMaps.push_back(values);
        or_total_loaded_rows += values.size();
    }
    
    size_t or_blocks_total_size = get_directory_size(originBasePath);
    auto t_phase12_end = chrono::steady_clock::now();
    double or_load_time = chrono::duration<double>(t_phase12_end - t_phase12_start).count();
    
    update_ram();
    log_msg("Phase 1-2 (load origin attributes): " + to_string(or_load_time) + " s\n");
    log_msg("  Origin total loaded rows: " + to_string(or_total_loaded_rows) + "\n");

    // === PHASE 3-4: Load ALL destination attributes ===
    auto t_phase34_start = chrono::steady_clock::now();
    
    vector<unordered_map<uint32_t, float>> destMaps;
    size_t dst_total_loaded_rows = 0;
    
    for (uint32_t attr : destAttrNums) {
        auto values = load_attribute_values(destBasePath, attr);
        destMaps.push_back(values);
        dst_total_loaded_rows += values.size();
    }
    
    size_t dst_blocks_total_size = get_directory_size(destBasePath);
    auto t_phase34_end = chrono::steady_clock::now();
    double dst_load_time = chrono::duration<double>(t_phase34_end - t_phase34_start).count();
    
    update_ram();
    log_msg("Phase 3-4 (load dest attributes): " + to_string(dst_load_time) + " s\n");
    log_msg("  Destination total loaded rows: " + to_string(dst_total_loaded_rows) + "\n");

    // === PHASE 5: Load accessibility index ===
    auto t_phase5_start = chrono::steady_clock::now();
    
    unordered_map<uint32_t, AccIndexEntry> accIndexMap = load_accessibility_index(accBasePath);
    
    // Compute intersection of destination IDs
    set<uint32_t> dest_ids_set;
    for (const auto& [dest_id, _] : destMaps[0]) {
        bool in_all = true;
        for (size_t i = 1; i < destMaps.size(); i++) {
            if (destMaps[i].find(dest_id) == destMaps[i].end()) {
                in_all = false;
                break;
            }
        }
        if (in_all) dest_ids_set.insert(dest_id);
    }
    vector<uint32_t> selected_dest_ids(dest_ids_set.begin(), dest_ids_set.end());
    
    auto t_phase5_end = chrono::steady_clock::now();
    double acc_idx_load_time = chrono::duration<double>(t_phase5_end - t_phase5_start).count();
    
    update_ram();
    log_msg("Phase 5 (load accessibility index): " + to_string(acc_idx_load_time) + " s\n");
    log_msg("  Selected destinations (intersection): " + to_string(selected_dest_ids.size()) + "\n");

    // === PHASE 6: Load accessibility blocks ===
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
    
    size_t acc_blocks_total_size = get_directory_size(accBasePath);
    auto t_phase6_end = chrono::steady_clock::now();
    double acc_bin_load_time = chrono::duration<double>(t_phase6_end - t_phase6_start).count();
    
    update_ram();
    log_msg("Phase 6 (load accessibility blocks): " + to_string(acc_bin_load_time) + " s\n");
    log_msg("  Accessibility loaded rows: " + to_string(acc_bin_loaded_rows) + "\n");

    // === PHASE 7: Filtering with multi-attribute AND logic ===
    auto t_phase7_start = chrono::steady_clock::now();
    
    vector<thread> threads;
    mutex results_mutex;
    vector<Accessibility> filtered_results;
    
    auto process_dest_range = [&](size_t start, size_t end) {
        vector<Accessibility> local_results;
        
        for (size_t i = start; i < end; ++i) {
            uint32_t dest_id = selected_dest_ids[i];
            auto data_it = loaded_acc_data.find(dest_id);
            if (data_it == loaded_acc_data.end()) continue;
            
            for (const auto& a : data_it->second) {
                // Check if origin_id exists in ALL origin attribute maps (AND logic)
                bool origin_match = true;
                for (const auto& originMap : originMaps) {
                    if (originMap.find(a.origin_id) == originMap.end()) {
                        origin_match = false;
                        break;
                    }
                }
                
                if (origin_match) {
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
    
    size_t result_acc_rows = filtered_results.size();
    auto t_phase7_end = chrono::steady_clock::now();
    double time_filtering = chrono::duration<double>(t_phase7_end - t_phase7_start).count();
    size_t result_acc_size = result_acc_rows * sizeof(Accessibility);
    
    update_ram();
    log_msg("Phase 7 (filtering): " + to_string(time_filtering) + " s\n");
    log_msg("  Result rows: " + to_string(result_acc_rows) + "\n");

    // === PHASE 8: Write results ===
    auto t_phase8_start = chrono::steady_clock::now();
    
    ofstream fout(outputPath, ios::binary);
    fout.write(reinterpret_cast<const char*>(filtered_results.data()), 
               filtered_results.size() * sizeof(Accessibility));
    fout.close();
    
    auto t_phase8_end = chrono::steady_clock::now();
    double time_write_bin = chrono::duration<double>(t_phase8_end - t_phase8_start).count();
    
    update_ram();
    log_msg("Phase 8 (write binary results): " + to_string(time_write_bin) + " s\n");

    // === Generate report ===
    auto t_total_end = chrono::steady_clock::now();
    double total_time = chrono::duration<double>(t_total_end - t_total_start).count();
    
    ofstream report(reportPath);
    report << "========================================\n";
    report << "QUERY RESULT REPORT (MULTI-ATTRIBUTE)\n";
    report << "========================================\n\n";
    report << "Binary file: " << outputPath << "\n";
    report << "Dataset percentage: " << percent << "%\n";
    report << "Origin attributes: " << originAttrsStr << "\n";
    report << "Destination attributes: " << destAttrsStr << "\n";
    report << "Filter logic: AND (all attributes must have non-null value)\n\n";
    
    report << "========================================\n";
    report << "PERFORMANCE SUMMARY\n";
    report << "========================================\n";
    report << "Total time: " << fixed << setprecision(4) << total_time << " s\n";
    report << "  - Load origin attributes: " << or_load_time << " s\n";
    report << "  - Load dest attributes: " << dst_load_time << " s\n";
    report << "  - Load accessibility index: " << acc_idx_load_time << " s\n";
    report << "  - Load accessibility data: " << acc_bin_load_time << " s\n";
    report << "  - Filtering: " << time_filtering << " s\n";
    report << "  - Write binary: " << time_write_bin << " s\n\n";
    
    report << "========================================\n";
    report << "DATA STATISTICS\n";
    report << "========================================\n";
    report << "Origin attributes loaded: " << or_total_loaded_rows << " rows (" << originAttrNums.size() << " attrs)\n";
    report << "Destination attributes loaded: " << dst_total_loaded_rows << " rows (" << destAttrNums.size() << " attrs)\n";
    report << "Accessibility records loaded: " << acc_bin_loaded_rows << " rows\n";
    report << "Result records: " << result_acc_rows << " rows\n";
    report << "Selectivity: " << fixed << setprecision(2) 
           << (100.0 * result_acc_rows / (acc_bin_loaded_rows > 0 ? acc_bin_loaded_rows : 1)) << "%\n";
    
    report << "\n========================================\n";
    report << "RAM USAGE\n";
    report << "========================================\n";
    report << "RAM Min: " << ram_min << " bytes\n";
    report << "RAM Max: " << ram_max << " bytes\n";
    report << "RAM Delta: " << (ram_max - ram_min) << " bytes\n";
    report << "========================================\n";
    
    report.close();

    // === Final output ===
    log_msg("\n=== FINAL SUMMARY ===\n");
    log_msg("Total time: " + to_string(total_time) + " s\n");
    log_msg("Binary result: " + outputPath + "\n");
    log_msg("Report: " + reportPath + "\n");
    
    cout << percent << "," << originAttrsStr << "," << destAttrsStr << ","
         << or_total_loaded_rows << "," << dst_total_loaded_rows << "," << acc_bin_loaded_rows << ","
         << or_load_time << "," << dst_load_time << "," << acc_bin_load_time << ","
         << time_filtering << "," << time_write_bin << "," << total_time << ","
         << result_acc_size << ","
         << num_threads << "," << ram_min << "," << ram_max << "," << (ram_max - ram_min) << "\n";
    
    cout << "\nQuery done!" << endl;
    
    return 0;
}
