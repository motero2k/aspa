#include <bits/stdc++.h>
#include <filesystem>
using namespace std;

constexpr uint32_t ORIGIN_ATTRS = 5000;
constexpr uint32_t DEST_ATTRS   = 2000;
constexpr uint32_t ATTRS_PER_BLOCK = 100;
constexpr uint32_t BUFFER_SIZE = 1024 * 1024; // 1MB buffer

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

int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Usage: ./preprocess_dataset <input_dir> <percent>\n";
        return 1;
    }
    string inDir = argv[1];
    float percent = stof(argv[2]);
    int percent_int = static_cast<int>(percent * 100 + 0.5f);
    string suffix = to_string(percent_int) + "p";

    // Create output directories with percentage suffix
    string outBase = "out/" + suffix;
    filesystem::create_directories(outBase + "/attributes/origin/blocks");
    cout << "Directory created: " << outBase + "/attributes/origin/blocks" << endl;

    filesystem::create_directories(outBase + "/attributes/destination/blocks");
    cout << "Directory created: " << outBase + "/attributes/destination/blocks" << endl;

    filesystem::create_directories(outBase + "/accessibility/blocks");
    cout << "Directory created: " << outBase + "/accessibility/blocks" << endl;

    // ===============================
    // 1️⃣  SIMPLIFIED EFFICIENT ATTRIBUTES IN BLOCKS
    // ===============================
    auto process_table = [&](string type, uint32_t n_attrs) {
        string binPath = inDir + "/" + type + "_" + suffix + ".bin";
        ifstream f(binPath, ios::binary);

        // Read total size
        f.seekg(0, ios::end);
        uint64_t total_bytes = f.tellg();
        f.seekg(0);

        uint64_t row_size = 4 + n_attrs * 4;
        uint32_t n_rows = total_bytes / row_size;

        cout << "Processing " << type << " table: " << n_rows << " rows, " << n_attrs << " attributes\n";

        string outType = (type == "dest") ? "destination" : type;
        string indexPath = outBase + "/attributes/" + outType + "/index.bin";
        
        // Read entire file into memory for faster processing
        vector<char> file_data(total_bytes);
        f.read(file_data.data(), total_bytes);
        f.close();
        
        cout << "  File loaded into memory (" << (total_bytes / (1024*1024)) << " MB)" << endl;

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
        
        cout << "  Data extracted from memory" << endl;

        // Write attributes in blocks efficiently
        ofstream indexFile(indexPath, ios::binary);
        uint32_t current_block = 0;
        ofstream currentBlockFile;
        uint32_t attrs_in_current_block = 0;

        for (uint32_t a = 0; a < n_attrs; ++a) {
            // Open new block file if needed
            if (attrs_in_current_block == 0) {
                if (currentBlockFile.is_open()) currentBlockFile.close();
                string blockPath = outBase + "/attributes/" + outType + "/blocks/block_" + to_string(current_block) + ".bin";
                currentBlockFile.open(blockPath, ios::binary);
                cout << "  → Writing block " << current_block << endl;
            }

            uint64_t offset_start = currentBlockFile.tellp();
            
            // Write all data for this attribute at once
            const auto& attr_data = all_attrs[a];
            currentBlockFile.write(reinterpret_cast<const char*>(attr_data.data()), 
                                 attr_data.size() * sizeof(pair<uint32_t, float>));

            // Write index entry
            AttributeIndex idx = {current_block, offset_start, static_cast<uint32_t>(attr_data.size())};
            indexFile.write(reinterpret_cast<const char*>(&idx), sizeof(idx));
            
            attrs_in_current_block++;
            if (attrs_in_current_block >= ATTRS_PER_BLOCK) {
                attrs_in_current_block = 0;
                current_block++;
            }
        }
        
        if (currentBlockFile.is_open()) currentBlockFile.close();
        indexFile.close();
        
        cout << "  → " << outType << " processing completed" << endl;
    };

    process_table("origin", ORIGIN_ATTRS);
    process_table("destination", DEST_ATTRS);

    // ===============================
    // 2️⃣  SIMPLIFIED ACCESSIBILITY BY BLOCKS
    // ===============================
    string accPath = inDir + "/accessibility_" + suffix + ".bin";
    ifstream f(accPath, ios::binary);

    constexpr uint32_t ORIGINS_PER_BLOCK = 1000;
    constexpr uint32_t BUFFER_RECORDS = 10000; // Buffer 10k records

    uint32_t currentOriginBlock = UINT32_MAX;
    ofstream outBlock;
    vector<Accessibility> buffer;
    buffer.reserve(BUFFER_RECORDS);
    
    uint64_t count = 0;
    Accessibility a;

    while (f.read(reinterpret_cast<char*>(&a), sizeof(a))) {
        uint32_t block = a.origin_id / ORIGINS_PER_BLOCK;
        
        if (block != currentOriginBlock) {
            // Flush buffer and switch blocks
            if (outBlock.is_open() && !buffer.empty()) {
                outBlock.write(reinterpret_cast<const char*>(buffer.data()), 
                              buffer.size() * sizeof(Accessibility));
                buffer.clear();
            }
            
            if (outBlock.is_open()) outBlock.close();
            currentOriginBlock = block;
            string outName = outBase + "/accessibility/blocks/block_" + to_string(currentOriginBlock) + ".bin";
            outBlock.open(outName, ios::binary);
            if (count % 1000000 == 0) {
                cout << "→ Block " << currentOriginBlock << " (processed " << count << " records)" << endl;
            }
        }
        
        buffer.push_back(a);
        
        // Flush when buffer is full
        if (buffer.size() >= BUFFER_RECORDS) {
            outBlock.write(reinterpret_cast<const char*>(buffer.data()), 
                          buffer.size() * sizeof(Accessibility));
            buffer.clear();
        }
        
        count++;
    }
    
    // Final flush
    if (outBlock.is_open() && !buffer.empty()) {
        outBlock.write(reinterpret_cast<const char*>(buffer.data()), 
                      buffer.size() * sizeof(Accessibility));
    }

    f.close();
    if (outBlock.is_open()) outBlock.close();
    cout << "Accessibility split completed. Total pairs: " << count << endl;

    return 0;
}
