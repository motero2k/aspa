#include <bits/stdc++.h>
#include <filesystem>
#include <thread>
#include <mutex>
#include <atomic>
using namespace std;

// ===============================================
// SCHEMA PARAMETERS (IMMUTABLE)
// ===============================================
// These values define the fundamental binary file structure.
// They MUST be known beforehand and cannot be auto-calculated
// from file size due to the unreliability of inferring binary
// formats at runtime.
constexpr uint32_t ORIGIN_ATTRS = 5000;
constexpr uint32_t DEST_ATTRS   = 2000;

// ===============================================
// OPTIMAL BLOCK SIZES FOR I/O EFFICIENCY
// ===============================================
// Targets chosen for Linux x64 I/O performance optimization
constexpr size_t TARGET_DEST_ATTR_BLOCK_SIZE_BYTES   = 8 * 1024 * 1024;    // 8 MB
constexpr size_t TARGET_ORIGIN_ATTR_BLOCK_SIZE_BYTES = 32 * 1024 * 1024;   // 32 MB
constexpr size_t TARGET_ACC_BLOCK_SIZE_BYTES         = 256 * 1024 * 1024;  // 256 MB

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

struct AccessibilityDestIndex {
    uint32_t destination_id;
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

// Thread-safe console output
mutex cout_mutex;
void thread_safe_print(const string& msg) {
    lock_guard<mutex> lock(cout_mutex);
    cout << msg << flush;
}

int main(int argc, char** argv) {
    if (argc < 4) {
        cerr << "Usage: ./preprocess_dataset <input_dir> <percent> <output_dir>\n";
        return 1;
    }
    string inDir = argv[1];
    float percent = stof(argv[2]);
    string outDir = argv[3];
    int percent_int = static_cast<int>(percent * 100 + 0.5f);
    string percent_str = to_string(percent_int) + "p";
    string suffix = percent_str;

    auto t_total_start = chrono::steady_clock::now();

    // Create output directories with percentage suffix
    string outBase = outDir + "/" + suffix;
    filesystem::create_directories(outBase + "/attributes/origin/blocks");
    cout << "Directory created: " << outBase + "/attributes/origin/blocks" << endl;

    filesystem::create_directories(outBase + "/attributes/destination/blocks");
    cout << "Directory created: " << outBase + "/attributes/destination/blocks" << endl;

    filesystem::create_directories(outBase + "/accessibility/blocks");
    cout << "Directory created: " << outBase + "/accessibility/blocks" << endl;

    // Variables for report - use atomic for thread safety
    atomic<uint64_t> origin_input_bytes{0}, dest_input_bytes{0}, acc_input_bytes{0};
    atomic<uint64_t> origin_output_bytes{0}, dest_output_bytes{0}, acc_output_bytes{0};
    atomic<uint32_t> origin_blocks{0}, dest_blocks{0}, acc_blocks{0};
    atomic<double> origin_time{0}, dest_time{0}, acc_time{0};
    atomic<size_t> acc_records_count{0};

    // ===============================
    // 1️⃣  SIZE-BASED ATTRIBUTE BLOCKING WITH MULTITHREADING
    // ===============================
    auto process_table = [&](string type, uint32_t n_attrs, atomic<uint64_t>& input_bytes, 
                             atomic<uint64_t>& output_bytes, atomic<uint32_t>& blocks_created, atomic<double>& proc_time) {
        auto t_start = chrono::steady_clock::now();
        
        string binPath = inDir + "/" + type + "_" + suffix + ".bin";
        ifstream f(binPath, ios::binary);

        // Read total size
        f.seekg(0, ios::end);
        uint64_t total_bytes = f.tellg();
        input_bytes = total_bytes;
        f.seekg(0);

        uint64_t row_size = 4 + n_attrs * 4;
        uint32_t n_rows = total_bytes / row_size;

        thread_safe_print("Processing " + type + " table: " + to_string(n_rows) + " rows, " + 
                         to_string(n_attrs) + " attributes\n");

        // Select target block size based on type
        size_t target_block_size = (type == "origin") ? TARGET_ORIGIN_ATTR_BLOCK_SIZE_BYTES 
                                                       : TARGET_DEST_ATTR_BLOCK_SIZE_BYTES;
        thread_safe_print("  Target block size: " + to_string(target_block_size / (1024*1024)) + " MB\n");

        string outType = (type == "destination") ? "destination" : type;
        string indexPath = outBase + "/attributes/" + outType + "/index.bin";
        
        // Read entire file into memory for faster processing
        vector<char> file_data(total_bytes);
        f.read(file_data.data(), total_bytes);
        f.close();
        
        thread_safe_print("  [" + type + "] File loaded into memory (" + 
                         to_string(total_bytes / (1024*1024)) + " MB)\n");

        // Extract all attribute data in memory first
        vector<vector<pair<uint32_t, float>>> all_attrs(n_attrs);
        
        for (uint32_t i = 0; i < n_rows; ++i) {
            uint64_t row_offset = i * row_size;
            uint32_t id = *reinterpret_cast<uint32_t*>(file_data.data() + row_offset);
            
            for (uint32_t a = 0; a < n_attrs; ++a) {
                float val = *reinterpret_cast<float*>(file_data.data() + row_offset + 4 + a * 4);
                if (!isnan(val)) {
                    all_attrs[a].emplace_back(id, val);
                }
            }
        }
        
        thread_safe_print("  [" + type + "] Data extracted from memory\n");

        // Write attributes in size-based blocks
        ofstream indexFile(indexPath, ios::binary);
        uint32_t current_block = 0;
        ofstream currentBlockFile;
        size_t current_block_bytes = 0;

        for (uint32_t a = 0; a < n_attrs; ++a) {
            const auto& attr_data = all_attrs[a];
            size_t attr_bytes = attr_data.size() * sizeof(pair<uint32_t, float>);
            
            // Check if adding this attribute would exceed block size
            if (current_block_bytes > 0 && current_block_bytes + attr_bytes > target_block_size) {
                // Close current block and start new one
                currentBlockFile.close();
                current_block++;
                current_block_bytes = 0;
            }
            
            // Open new block file if needed
            if (!currentBlockFile.is_open()) {
                string blockPath = outBase + "/attributes/" + outType + "/blocks/block_" + to_string(current_block) + ".bin";
                currentBlockFile.open(blockPath, ios::binary);
            }

            uint64_t offset_start = currentBlockFile.tellp();
            
            // Write all data for this attribute at once
            currentBlockFile.write(reinterpret_cast<const char*>(attr_data.data()), attr_bytes);
            current_block_bytes += attr_bytes;

            // Write index entry
            AttributeIndex idx = {current_block, offset_start, static_cast<uint32_t>(attr_data.size())};
            indexFile.write(reinterpret_cast<const char*>(&idx), sizeof(idx));
        }
        
        if (currentBlockFile.is_open()) currentBlockFile.close();
        indexFile.close();
        
        blocks_created = current_block + 1;
        
        // Calculate output size
        uint64_t total_output = 0;
        for (uint32_t b = 0; b <= current_block; ++b) {
            string blockPath = outBase + "/attributes/" + outType + "/blocks/block_" + to_string(b) + ".bin";
            if (filesystem::exists(blockPath)) {
                total_output += filesystem::file_size(blockPath);
            }
        }
        total_output += filesystem::file_size(indexPath);
        output_bytes = total_output;
        
        auto t_end = chrono::steady_clock::now();
        proc_time = chrono::duration<double>(t_end - t_start).count();
        
        thread_safe_print("  [" + type + "] → Processing completed in " + 
                         to_string(proc_time.load()) + " s\n");
        thread_safe_print("  [" + type + "] → Created " + to_string(blocks_created.load()) + " blocks\n");
    };

    // Launch parallel threads for attribute processing
    thread origin_thread([&]() {
        process_table("origin", ORIGIN_ATTRS, origin_input_bytes, origin_output_bytes, origin_blocks, origin_time);
    });
    
    thread dest_thread([&]() {
        process_table("destination", DEST_ATTRS, dest_input_bytes, dest_output_bytes, dest_blocks, dest_time);
    });

    // ===============================
    // 2️⃣  ACCESSIBILITY WITH SIZE-BASED BLOCKING
    // ===============================
    thread acc_thread([&]() {
        auto t_acc_start = chrono::steady_clock::now();
        
        string accPath = inDir + "/accessibility_" + suffix + ".bin";
        ifstream f(accPath, ios::binary);

        // Get input size
        f.seekg(0, ios::end);
        acc_input_bytes = f.tellg();
        f.seekg(0);

        // Read all accessibility records into memory
        vector<Accessibility> all_acc;
        Accessibility a;
        while (f.read(reinterpret_cast<char*>(&a), sizeof(a))) {
            all_acc.push_back(a);
        }
        f.close();
        acc_records_count = all_acc.size();
        thread_safe_print("Loaded " + to_string(all_acc.size()) + " accessibility records into memory.\n");

        // Sort by destination_id
        sort(all_acc.begin(), all_acc.end(), [](const Accessibility& a, const Accessibility& b) {
            return a.destination_id < b.destination_id;
        });

        // Write blocks and build index
        string accBlockDir = outBase + "/accessibility/blocks";
        string accIndexPath = outBase + "/accessibility/index.bin";
        ofstream indexFile(accIndexPath, ios::binary);

        size_t current_block_size = 0;
        uint32_t current_block_id = 0;
        ofstream blockFile;
        uint64_t block_offset = 0;

        uint32_t last_dest_id = UINT32_MAX;
        uint64_t dest_start_offset = 0;
        uint32_t dest_count = 0;

        for (size_t i = 0; i < all_acc.size(); ++i) {
            const Accessibility& rec = all_acc[i];
            if (blockFile.is_open() == false) {
                string blockPath = accBlockDir + "/block_" + to_string(current_block_id) + ".bin";
                blockFile.open(blockPath, ios::binary);
                block_offset = 0;
                current_block_size = 0;
            }

            // If new destination_id, record previous index entry
            if (rec.destination_id != last_dest_id && last_dest_id != UINT32_MAX) {
                AccIndexEntry idx = {last_dest_id, current_block_id, dest_start_offset, dest_count};
                indexFile.write(reinterpret_cast<const char*>(&idx), sizeof(idx));
                dest_start_offset = block_offset;
                dest_count = 0;
            }

            // Write record
            blockFile.write(reinterpret_cast<const char*>(&rec), sizeof(rec));
            block_offset += sizeof(rec);
            current_block_size += sizeof(rec);
            dest_count++;
            last_dest_id = rec.destination_id;

            // If block size exceeded, close and start new block
            if (current_block_size >= TARGET_ACC_BLOCK_SIZE_BYTES) {
                // Write last index entry for this block if needed
                AccIndexEntry idx = {last_dest_id, current_block_id, dest_start_offset, dest_count};
                indexFile.write(reinterpret_cast<const char*>(&idx), sizeof(idx));
                blockFile.close();
                current_block_id++;
                block_offset = 0;
                current_block_size = 0;
                dest_start_offset = 0;
                dest_count = 0;
                last_dest_id = UINT32_MAX;
            }
        }
        // Write last index entry
        if (dest_count > 0 && last_dest_id != UINT32_MAX) {
            AccIndexEntry idx = {last_dest_id, current_block_id, dest_start_offset, dest_count};
            indexFile.write(reinterpret_cast<const char*>(&idx), sizeof(idx));
        }
        if (blockFile.is_open()) blockFile.close();
        indexFile.close();

        acc_blocks = current_block_id + 1;
        
        // Calculate output size
        uint64_t total_output = filesystem::file_size(accIndexPath);
        for (uint32_t b = 0; b <= current_block_id; ++b) {
            string blockPath = accBlockDir + "/block_" + to_string(b) + ".bin";
            if (filesystem::exists(blockPath)) {
                total_output += filesystem::file_size(blockPath);
            }
        }
        acc_output_bytes = total_output;
        
        auto t_acc_end = chrono::steady_clock::now();
        acc_time = chrono::duration<double>(t_acc_end - t_acc_start).count();

        thread_safe_print("Accessibility blocks and index.bin written in " + 
                         to_string(acc_time.load()) + " s\n");
        thread_safe_print("Created " + to_string(acc_blocks.load()) + " blocks\n");
    });

    // Wait for all threads to complete
    origin_thread.join();
    dest_thread.join();
    acc_thread.join();

    auto t_total_end = chrono::steady_clock::now();
    double total_time = chrono::duration<double>(t_total_end - t_total_start).count();

    // ===============================
    // 3️⃣  GENERATE PROCESSING REPORT
    // ===============================
    string reportPath = outBase + "/preprocessing_report_" + percent_str + ".txt";
    ofstream report(reportPath);
    
    auto format_size = [](uint64_t bytes) -> string {
        char buf[64];
        if (bytes < 1024)
            snprintf(buf, sizeof(buf), "%lu B", bytes);
        else if (bytes < 1024 * 1024)
            snprintf(buf, sizeof(buf), "%.2f KiB", bytes / 1024.0);
        else if (bytes < 1024 * 1024 * 1024)
            snprintf(buf, sizeof(buf), "%.2f MiB", bytes / (1024.0 * 1024.0));
        else
            snprintf(buf, sizeof(buf), "%.2f GiB", bytes / (1024.0 * 1024.0 * 1024.0));
        return buf;
    };
    
    report << "========================================\n";
    report << "PREPROCESSING REPORT\n";
    report << "========================================\n\n";
    report << "Dataset percentage: " << percent_int << "%\n";
    report << "Input directory: " << inDir << "\n";
    report << "Output directory: " << outBase << "\n";
    report << "Total processing time: " << total_time << " seconds\n\n";
    
    report << "========================================\n";
    report << "BLOCK SIZE CONFIGURATION\n";
    report << "========================================\n";
    report << "Origin attributes: " << (TARGET_ORIGIN_ATTR_BLOCK_SIZE_BYTES / (1024*1024)) << " MB per block\n";
    report << "Destination attributes: " << (TARGET_DEST_ATTR_BLOCK_SIZE_BYTES / (1024*1024)) << " MB per block\n";
    report << "Accessibility: " << (TARGET_ACC_BLOCK_SIZE_BYTES / (1024*1024)) << " MB per block\n\n";
    
    report << "========================================\n";
    report << "ORIGIN ATTRIBUTES\n";
    report << "========================================\n";
    report << "Input file size: " << format_size(origin_input_bytes) << " (" << origin_input_bytes << " bytes)\n";
    report << "Output size: " << format_size(origin_output_bytes) << " (" << origin_output_bytes << " bytes)\n";
    report << "Blocks created: " << origin_blocks << "\n";
    report << "Processing time: " << origin_time << " seconds\n";
    report << "Compression ratio: " << fixed << setprecision(2) << (100.0 * origin_output_bytes / origin_input_bytes) << "%\n\n";
    
    report << "========================================\n";
    report << "DESTINATION ATTRIBUTES\n";
    report << "========================================\n";
    report << "Input file size: " << format_size(dest_input_bytes) << " (" << dest_input_bytes << " bytes)\n";
    report << "Output size: " << format_size(dest_output_bytes) << " (" << dest_output_bytes << " bytes)\n";
    report << "Blocks created: " << dest_blocks << "\n";
    report << "Processing time: " << dest_time << " seconds\n";
    report << "Compression ratio: " << fixed << setprecision(2) << (100.0 * dest_output_bytes / dest_input_bytes) << "%\n\n";
    
    report << "========================================\n";
    report << "ACCESSIBILITY\n";
    report << "========================================\n";
    report << "Input file size: " << format_size(acc_input_bytes) << " (" << acc_input_bytes << " bytes)\n";
    report << "Output size: " << format_size(acc_output_bytes) << " (" << acc_output_bytes << " bytes)\n";
    report << "Records processed: " << acc_records_count << "\n";
    report << "Blocks created: " << acc_blocks << "\n";
    report << "Processing time: " << acc_time << " seconds\n";
    report << "Overhead ratio: " << fixed << setprecision(2) << (100.0 * acc_output_bytes / acc_input_bytes) << "%\n\n";
    
    uint64_t total_input = origin_input_bytes + dest_input_bytes + acc_input_bytes;
    uint64_t total_output = origin_output_bytes + dest_output_bytes + acc_output_bytes;
    
    report << "========================================\n";
    report << "SUMMARY\n";
    report << "========================================\n";
    report << "Total input: " << format_size(total_input) << " (" << total_input << " bytes)\n";
    report << "Total output: " << format_size(total_output) << " (" << total_output << " bytes)\n";
    report << "Total blocks: " << (origin_blocks + dest_blocks + acc_blocks) << "\n";
    report << "Overall ratio: " << fixed << setprecision(2) << (100.0 * total_output / total_input) << "%\n";
    report << "========================================\n";
    
    report.close();
    
    cout << "\n========================================" << endl;
    cout << "Preprocessing completed successfully!" << endl;
    cout << "Total time: " << total_time << " seconds" << endl;
    cout << "Report generated: " << reportPath << endl;
    cout << "========================================" << endl;

    return 0;
}
