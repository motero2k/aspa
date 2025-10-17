#include <bits/stdc++.h>
#include <filesystem>
#include <thread>
#include <mutex>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
using namespace std;

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

int main(int argc, char** argv) {
    if (argc < 5) {
        cerr << "Usage: ./query_filter <percent> <origin_attr_name> <dest_attr_name> <output.txt>\n";
        return 1;
    }

    string percent = argv[1];
    string originAttr = argv[2];
    string destAttr = argv[3];
    string outputPath = argv[4];
    string suffix = percent + "p";

    auto t_start = chrono::steady_clock::now();

    // === 1️⃣ Load filtered attributes
    auto t_attr_start = chrono::steady_clock::now();
    
    // Extract attribute numbers from names (e.g., "att3" -> 3)
    uint32_t originAttrNum = stoi(originAttr.substr(3));
    uint32_t destAttrNum = stoi(destAttr.substr(3));
    
    // Use percentage-specific paths
    string outBase = "out/" + suffix;
    string originBasePath = outBase + "/attributes/origin";
    string destBasePath = outBase + "/attributes/destination";

    auto originValues = load_attribute_values(originBasePath, originAttrNum);
    auto destValues = load_attribute_values(destBasePath, destAttrNum);

    auto t_attr_end = chrono::steady_clock::now();
    cout << "Origins loaded: " << originValues.size()
         << "  Destinations: " << destValues.size() << endl;
    cout << "Phase 1 (load attributes): "
         << chrono::duration<double>(t_attr_end - t_attr_start).count() << " s" << endl;

    // === 2️⃣ Filter accessibility (multithreaded, detailed timing)
    auto t_acc_start = chrono::steady_clock::now();

    ofstream fout(outputPath);
    fout << "origin_id,destination_id,time,distance," << originAttr << "," << destAttr << "\n";

    string accessibilityBlocksPath = outBase + "/accessibility/blocks";
    vector<string> block_files;
    for (const auto& entry : filesystem::directory_iterator(accessibilityBlocksPath)) {
        block_files.push_back(entry.path());
    }
    sort(block_files.begin(), block_files.end());

    unsigned int n_threads = thread::hardware_concurrency();
    if (n_threads == 0) n_threads = 4;
    vector<vector<string>> thread_results(n_threads);

    // Timing variables
    vector<double> thread_read_times(n_threads, 0.0);
    vector<double> thread_filter_times(n_threads, 0.0);

    auto worker = [&](int tid) {
        auto t_read_start = chrono::steady_clock::now();
        vector<string> local_lines;
        for (size_t i = tid; i < block_files.size(); i += n_threads) {
            auto t_file_read_start = chrono::steady_clock::now();
            ifstream f(block_files[i], ios::binary);
            vector<Accessibility> records;
            Accessibility a;
            while (f.read((char*)&a, sizeof(a))) {
                records.push_back(a);
            }
            auto t_file_read_end = chrono::steady_clock::now();
            thread_read_times[tid] += chrono::duration<double>(t_file_read_end - t_file_read_start).count();

            auto t_filter_start = chrono::steady_clock::now();
            for (const auto& rec : records) {
                auto originIt = originValues.find(rec.origin_id);
                auto destIt = destValues.find(rec.destination_id);
                if (originIt != originValues.end() && destIt != destValues.end()) {
                    ostringstream oss;
                    oss << rec.origin_id << "," << rec.destination_id << ","
                        << rec.time << "," << rec.distance << ","
                        << originIt->second << "," << destIt->second << "\n";
                    local_lines.push_back(oss.str());
                }
            }
            auto t_filter_end = chrono::steady_clock::now();
            thread_filter_times[tid] += chrono::duration<double>(t_filter_end - t_filter_start).count();
        }
        thread_results[tid] = move(local_lines);
    };

    auto t_threads_start = chrono::steady_clock::now();
    vector<thread> threads;
    for (unsigned int t = 0; t < n_threads; ++t) {
        threads.emplace_back(worker, t);
    }
    for (auto& th : threads) th.join();
    auto t_threads_end = chrono::steady_clock::now();

    auto t_write_start = chrono::steady_clock::now();
    size_t total_lines = 0;
    for (const auto& lines : thread_results) {
        total_lines += lines.size();
        for (const auto& line : lines) {
            fout << line;
        }
    }
    fout.close();
    auto t_write_end = chrono::steady_clock::now();

    auto t_acc_end = chrono::steady_clock::now();

    // Print detailed timings
    double total_read = accumulate(thread_read_times.begin(), thread_read_times.end(), 0.0);
    double total_filter = accumulate(thread_filter_times.begin(), thread_filter_times.end(), 0.0);
    double total_write = chrono::duration<double>(t_write_end - t_write_start).count();
    double total_threads = chrono::duration<double>(t_threads_end - t_threads_start).count();
    double total_acc = chrono::duration<double>(t_acc_end - t_acc_start).count();

    cout << "Phase 2 breakdown:" << endl;
    cout << "  Reading accessibility blocks: " << total_read << " s" << endl;
    cout << "  Filtering records: " << total_filter << " s" << endl;
    cout << "  Writing results: " << total_write << " s" << endl;
    cout << "  Threads total (read+filter): " << total_threads << " s" << endl;
    cout << "  Phase 2 (overall): " << total_acc << " s" << endl;
    cout << "  Total filtered records: " << total_lines << endl;

    auto t_end = chrono::steady_clock::now();
    cout << "Total time: "
         << chrono::duration<double>(t_end - t_start).count() << " s" << endl;

    cout << "Query done. Results written to: " << outputPath << endl;
}
