#include <bits/stdc++.h>
#include <filesystem>
#include <thread>
#include <mutex>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
using namespace std;

// Accessibility record structure
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

int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Usage: ./lab <percent> <dest_attr_name>\n";
        return 1;
    }
    string percent = argv[1];
    string destAttr = argv[2];
    string suffix = percent + "p";
    string outBase = "out/" + suffix;
    string destBasePath = outBase + "/attributes/destination";
    string accBasePath = outBase + "/accessibility";

    // Repeat the process several times
    int repetitions = 3;
    for (int rep = 1; rep <= repetitions; ++rep) {
        cout << "=== Repetition " << rep << " ===" << endl;

        auto t_phase_start = chrono::steady_clock::now();

        // Phase 0: Load destination attributes
        auto t_attr_start = chrono::steady_clock::now();
        uint32_t destAttrNum = stoi(destAttr.substr(3));
        auto destValues = load_attribute_values(destBasePath, destAttrNum);
        auto t_attr_end = chrono::steady_clock::now();
        cout << "Loaded " << destValues.size() << " destination attribute values." << endl;
        cout << "Attribute loading time: "
             << chrono::duration<double>(t_attr_end - t_attr_start).count() << " s" << endl;

        // Load accessibility index
        auto t_index_start = chrono::steady_clock::now();
        auto accIndexMap = load_accessibility_index(accBasePath);
        auto t_index_end = chrono::steady_clock::now();
        cout << "Loaded accessibility index for " << accIndexMap.size() << " destinations." << endl;
        cout << "Accessibility index loading time: "
             << chrono::duration<double>(t_index_end - t_index_start).count() << " s" << endl;

        // Get all destination_ids
        vector<uint32_t> dest_ids;
        for (const auto& [dest_id, _] : accIndexMap) {
            dest_ids.push_back(dest_id);
        }

        // Phase 1: Read and count all records (no operation)
        auto t_phase1_start = chrono::steady_clock::now();
        size_t total_count1 = 0;
        size_t num_threads = thread::hardware_concurrency();
        vector<thread> threads1;
        mutex count_mutex1;

        auto process_dest_range1 = [&](size_t start, size_t end) {
            size_t local_count = 0;
            for (size_t i = start; i < end; ++i) {
                uint32_t dest_id = dest_ids[i];
                auto it = accIndexMap.find(dest_id);
                if (it == accIndexMap.end()) continue;
                auto records = load_accessibility_block(accBasePath, it->second);
                local_count += records.size();
            }
            lock_guard<mutex> lock(count_mutex1);
            total_count1 += local_count;
        };

        size_t total = dest_ids.size();
        size_t chunk = (total + num_threads - 1) / num_threads;
        for (size_t t = 0; t < num_threads; ++t) {
            size_t start = t * chunk;
            size_t end = min(start + chunk, total);
            if (start >= end) break;
            threads1.emplace_back(process_dest_range1, start, end);
        }
        for (auto& th : threads1) th.join();

        auto t_phase1_end = chrono::steady_clock::now();
        cout << "[Phase 1] Total records counted: " << total_count1 << endl;
        cout << "[Phase 1] Time: "
             << chrono::duration<double>(t_phase1_end - t_phase1_start).count() << " s" << endl;

        // Phase 2: Read and count, plus check if destination_id is in destValues
        auto t_phase2_start = chrono::steady_clock::now();
        size_t total_count2 = 0;
        size_t found_count = 0;
        vector<thread> threads2;
        mutex count_mutex2;

        auto process_dest_range2 = [&](size_t start, size_t end) {
            size_t local_count = 0;
            size_t local_found = 0;
            for (size_t i = start; i < end; ++i) {
                uint32_t dest_id = dest_ids[i];
                auto it = accIndexMap.find(dest_id);
                if (it == accIndexMap.end()) continue;
                auto records = load_accessibility_block(accBasePath, it->second);
                for (const auto& a : records) {
                    local_count++;
                    if (destValues.find(a.destination_id) != destValues.end()) {
                        local_found++;
                    }
                }
            }
            lock_guard<mutex> lock(count_mutex2);
            total_count2 += local_count;
            found_count += local_found;
        };

        for (size_t t = 0; t < num_threads; ++t) {
            size_t start = t * chunk;
            size_t end = min(start + chunk, total);
            if (start >= end) break;
            threads2.emplace_back(process_dest_range2, start, end);
        }
        for (auto& th : threads2) th.join();

        auto t_phase2_end = chrono::steady_clock::now();
        cout << "[Phase 2] Total records counted: " << total_count2 << endl;
        cout << "[Phase 2] Records where destination_id is in attribute set: " << found_count << endl;
        cout << "[Phase 2] Time: "
             << chrono::duration<double>(t_phase2_end - t_phase2_start).count() << " s" << endl;

        auto t_phase_end = chrono::steady_clock::now();
        cout << "Total repetition time: "
             << chrono::duration<double>(t_phase_end - t_phase_start).count() << " s" << endl;
        cout << endl;
    }
}
