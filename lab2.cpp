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
    if (argc < 4) {
        cerr << "Usage: ./lab2 <percent> <origin_attr_name> <dest_attr_name>\n";
        return 1;
    }
    string percent = argv[1];
    string originAttr = argv[2];
    string destAttr = argv[3];
    string suffix = percent + "p";
    string outBase = "out/" + suffix;
    string originBasePath = outBase + "/attributes/origin";
    string destBasePath = outBase + "/attributes/destination";
    string accBasePath = outBase + "/accessibility";

    int repetitions = 2;
    for (int rep = 1; rep <= repetitions; ++rep) {
        cout << "=== Repetition " << rep << " ===" << endl;

        auto t_phase_start = chrono::steady_clock::now();

        // Load origin and destination attributes
        auto t_attr_start = chrono::steady_clock::now();
        uint32_t originAttrNum = stoi(originAttr.substr(3));
        uint32_t destAttrNum = stoi(destAttr.substr(3));
        auto originValues = load_attribute_values(originBasePath, originAttrNum);
        auto destValues = load_attribute_values(destBasePath, destAttrNum);
        auto t_attr_end = chrono::steady_clock::now();
        cout << "Loaded " << originValues.size() << " origin attribute values." << endl;
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

        // Only process selected destination IDs (efficient reading)
        vector<uint32_t> selected_dest_ids;
        for (const auto& [dest_id, _] : destValues) {
            selected_dest_ids.push_back(dest_id);
        }

        // Count all records for selected destinations and origins
        auto t_count_start = chrono::steady_clock::now();
        size_t total_count = 0;
        size_t num_threads = thread::hardware_concurrency();
        vector<thread> threads;
        mutex count_mutex;

        auto process_dest_range = [&](size_t start, size_t end) {
            size_t local_count = 0;
            for (size_t i = start; i < end; ++i) {
                uint32_t dest_id = selected_dest_ids[i];
                auto it = accIndexMap.find(dest_id);
                if (it == accIndexMap.end()) continue;
                auto records = load_accessibility_block(accBasePath, it->second);
                for (const auto& a : records) {
                    if (originValues.find(a.origin_id) != originValues.end()) {
                        local_count++;
                    }
                }
            }
            lock_guard<mutex> lock(count_mutex);
            total_count += local_count;
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

        auto t_count_end = chrono::steady_clock::now();
        cout << "Total records counted (origin & destination match): " << total_count << endl;
        cout << "Counting time: "
             << chrono::duration<double>(t_count_end - t_count_start).count() << " s" << endl;

        auto t_phase_end = chrono::steady_clock::now();
        cout << "Total repetition time: "
             << chrono::duration<double>(t_phase_end - t_phase_start).count() << " s" << endl;
        cout << endl;
    }
}