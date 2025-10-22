#include <bits/stdc++.h>
using namespace std;

constexpr uint32_t NUM_ORIGINS = 221571;
constexpr uint32_t NUM_DESTS   = 16823;
constexpr uint32_t ORIGIN_ATTRS = 5000;
constexpr uint32_t DEST_ATTRS   = 2000;

struct Accessibility {
    uint32_t origin_id;
    uint32_t destination_id;
    float time;
    float distance;
};

inline float frand(float a=0.0f, float b=1.0f) {
    return a + static_cast<float>(rand()) / RAND_MAX * (b - a);
}

// Returns the exact number of non-nulls for a column given the percentage and total rows
inline uint64_t compute_non_nulls(uint32_t percent, uint32_t total_rows) {
    if (percent >= 100) return total_rows;
    return (total_rows * percent) / 100;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Usage: ./generate_full_dataset <output_dir> <percent>\n";
        return 1;
    }

    string outdir = argv[1];
    float percent = stof(argv[2]);
    if (percent <= 0.0f || percent > 1.0f) {
        cerr << "Percent must be in (0,1].\n";
        return 1;
    }
    // Calculate percent string for filenames (e.g., 10p for 0.1)
    int percent_int = static_cast<int>(percent * 100 + 0.5f);
    string percent_str = to_string(percent_int) + "p";

    // Always use seed 33
    srand(33);

    filesystem::create_directories(outdir);

    // Calculate scaled sizes
    uint32_t num_origins = static_cast<uint32_t>(NUM_ORIGINS * percent);
    uint32_t num_dests   = static_cast<uint32_t>(NUM_DESTS * percent);

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

    // Variables for report
    uint64_t origin_bytes = 0, dest_bytes = 0, acc_bytes = 0;
    uint64_t origin_rows = 0, dest_rows = 0, acc_rows = 0;

    // -------------------
    // ORIGIN TABLE
    // -------------------
    {
        string bin_path = outdir + "/origin_" + percent_str + ".bin";
        string txt_path = outdir + "/origin_" + percent_str + "_preview.txt";
        string origin_report_path = outdir + "/origin_" + percent_str + "_report.txt";

        ofstream f_bin(bin_path, ios::binary);
        ofstream f_txt(txt_path);

        // Write header
        f_txt << "id";
        for (uint32_t a = 0; a < ORIGIN_ATTRS; ++a)
            f_txt << ",att" << a;
        f_txt << "\n";

        // Write data
        for (uint32_t id = 0; id < num_origins; ++id) {
            f_bin.write((char*)&id, 4);
            if (id < 100) f_txt << id;
            for (uint32_t a = 0; a < ORIGIN_ATTRS; ++a) {
                float percent_non_null = (a == 100) ? 1.0f : (a < 100 ? a / 100.0f : 0.0f);
                uint64_t n_not_nulls = uint64_t(ceil(percent_non_null * num_origins));
                if (a >= 100) n_not_nulls = num_origins;
                if (n_not_nulls > num_origins) n_not_nulls = num_origins;
                float val;
                if (id < num_origins - n_not_nulls) {
                    val = NAN;
                    f_bin.write((char*)&val, 4);
                    if (id < 100) f_txt << ",NA";
                } else {
                    val = frand(0, 1000);
                    f_bin.write((char*)&val, 4);
                    if (id < 100) f_txt << "," << val;
                }
            }
            if (id < 100) f_txt << "\n";
        }
        origin_rows = num_origins;
        uint64_t origin_cells = uint64_t(num_origins) * (1 + ORIGIN_ATTRS);
        origin_bytes = origin_cells * 4;
        
        f_bin.close();
        f_txt.close();
        
        cout << "Origin file created: " << bin_path << endl;
        cout << "  Rows: " << num_origins << ", Columns: " << (1 + ORIGIN_ATTRS)
             << ", Bytes/cell: 4, Total: " << format_size(origin_bytes) << endl;
        
        // Generate origin report
        ofstream origin_report(origin_report_path);
        
        origin_report << "========================================\n";
        origin_report << "ORIGIN TABLE REPORT\n";
        origin_report << "========================================\n\n";
        origin_report << "Dataset percentage: " << percent_int << "%\n";
        origin_report << "Generated: " << __DATE__ << " " << __TIME__ << "\n\n";
        
        origin_report << "FILE INFORMATION:\n";
        origin_report << "  Binary file: origin_" << percent_str << ".bin\n";
        origin_report << "  Preview file: origin_preview_" << percent_str << ".txt\n";
        origin_report << "  File size: " << format_size(origin_bytes) << " (" << origin_bytes << " bytes)\n\n";
        
        origin_report << "TABLE STRUCTURE:\n";
        origin_report << "  Rows: " << origin_rows << "\n";
        origin_report << "  Columns: " << (1 + ORIGIN_ATTRS) << "\n";
        origin_report << "    - 1 ID column (uint32_t)\n";
        origin_report << "    - " << ORIGIN_ATTRS << " attribute columns (float)\n";
        origin_report << "  Total cells: " << origin_cells << "\n\n";
        
        origin_report << "BINARY FORMAT:\n";
        origin_report << "  Row layout: [uint32_t id][float att0]...[float att" << (ORIGIN_ATTRS-1) << "]\n";
        origin_report << "  Bytes per row: " << (4 + ORIGIN_ATTRS * 4) << " bytes\n";
        origin_report << "  Data type sizes:\n";
        origin_report << "    - ID: 4 bytes (uint32_t)\n";
        origin_report << "    - Attribute: 4 bytes (float)\n";
        origin_report << "  Endianness: Native system endianness\n";
        origin_report << "  No padding between fields\n\n";
        
        origin_report << "NULL DISTRIBUTION:\n";
        origin_report << "  Columns att0-att99: Progressive null percentage\n";
        origin_report << "    - att0: 100% nulls (0 non-null values)\n";
        origin_report << "    - att1: 99% nulls (1% non-null)\n";
        origin_report << "    - att50: 50% nulls (50% non-null)\n";
        origin_report << "    - att99: 1% nulls (99% non-null)\n";
        origin_report << "  Columns att100-att" << (ORIGIN_ATTRS-1) << ": No nulls (100% non-null)\n";
        origin_report << "  NULL representation: NaN (IEEE 754 float)\n\n";
        
        origin_report << "DATA RANGES:\n";
        origin_report << "  ID values: 0 to " << (num_origins - 1) << "\n";
        origin_report << "  Attribute values (non-null): 0.0 to 1000.0\n";
        origin_report << "  Random seed: 33\n\n";
        
        origin_report << "========================================\n";
        origin_report.close();
        
        cout << "  Preview: " << txt_path << endl;
        cout << "  Report: " << origin_report_path << endl;
    }

    // -------------------
    // DESTINATION TABLE
    // -------------------
    {
        string bin_path = outdir + "/destination_" + percent_str + ".bin";
        string txt_path = outdir + "/destination_" + percent_str + "_preview.txt";
        string dest_report_path = outdir + "/destination_" + percent_str + "_report.txt";
        
        ofstream f_bin(bin_path, ios::binary);
        ofstream f_txt(txt_path);

        // Write header
        f_txt << "id";
        for (uint32_t a = 0; a < DEST_ATTRS; ++a)
            f_txt << ",att" << a;
        f_txt << "\n";

        // Write data
        for (uint32_t id = 0; id < num_dests; ++id) {
            f_bin.write((char*)&id, 4);
            if (id < 100) f_txt << id;
            for (uint32_t a = 0; a < DEST_ATTRS; ++a) {
                float percent_non_null = (a == 100) ? 1.0f : (a < 100 ? a / 100.0f : 0.0f);
                uint64_t n_not_nulls = uint64_t(ceil(percent_non_null * num_dests));
                if (a >= 100) n_not_nulls = num_dests;
                if (n_not_nulls > num_dests) n_not_nulls = num_dests;
                float val;
                if (id < num_dests - n_not_nulls) {
                    val = NAN;
                    f_bin.write((char*)&val, 4);
                    if (id < 100) f_txt << ",NA";
                } else {
                    val = frand(0, 500);
                    f_bin.write((char*)&val, 4);
                    if (id < 100) f_txt << "," << val;
                }
            }
            if (id < 100) f_txt << "\n";
        }
        dest_rows = num_dests;
        uint64_t dest_cells = uint64_t(num_dests) * (1 + DEST_ATTRS);
        dest_bytes = dest_cells * 4;
        
        f_bin.close();
        f_txt.close();
        
        cout << "Destination file created: " << bin_path << endl;
        cout << "  Rows: " << num_dests << ", Columns: " << (1 + DEST_ATTRS)
             << ", Bytes/cell: 4, Total: " << format_size(dest_bytes) << endl;
        
        // Generate destination report
        ofstream dest_report(dest_report_path);
        
        dest_report << "========================================\n";
        dest_report << "DESTINATION TABLE REPORT\n";
        dest_report << "========================================\n\n";
        dest_report << "Dataset percentage: " << percent_int << "%\n";
        dest_report << "Generated: " << __DATE__ << " " << __TIME__ << "\n\n";
        
        dest_report << "FILE INFORMATION:\n";
        dest_report << "  Binary file: destination_" << percent_str << ".bin\n";
        dest_report << "  Preview file: destination_preview_" << percent_str << ".txt\n";
        dest_report << "  File size: " << format_size(dest_bytes) << " (" << dest_bytes << " bytes)\n\n";
        
        dest_report << "TABLE STRUCTURE:\n";
        dest_report << "  Rows: " << dest_rows << "\n";
        dest_report << "  Columns: " << (1 + DEST_ATTRS) << "\n";
        dest_report << "    - 1 ID column (uint32_t)\n";
        dest_report << "    - " << DEST_ATTRS << " attribute columns (float)\n";
        dest_report << "  Total cells: " << dest_cells << "\n\n";
        
        dest_report << "BINARY FORMAT:\n";
        dest_report << "  Row layout: [uint32_t id][float att0]...[float att" << (DEST_ATTRS-1) << "]\n";
        dest_report << "  Bytes per row: " << (4 + DEST_ATTRS * 4) << " bytes\n";
        dest_report << "  Data type sizes:\n";
        dest_report << "    - ID: 4 bytes (uint32_t)\n";
        dest_report << "    - Attribute: 4 bytes (float)\n";
        dest_report << "  Endianness: Native system endianness\n";
        dest_report << "  No padding between fields\n\n";
        
        dest_report << "NULL DISTRIBUTION:\n";
        dest_report << "  Columns att0-att99: Progressive null percentage\n";
        dest_report << "    - att0: 100% nulls (0 non-null values)\n";
        dest_report << "    - att1: 99% nulls (1% non-null)\n";
        dest_report << "    - att50: 50% nulls (50% non-null)\n";
        dest_report << "    - att99: 1% nulls (99% non-null)\n";
        dest_report << "  Columns att100-att" << (DEST_ATTRS-1) << ": No nulls (100% non-null)\n";
        dest_report << "  NULL representation: NaN (IEEE 754 float)\n\n";
        
        dest_report << "DATA RANGES:\n";
        dest_report << "  ID values: 0 to " << (num_dests - 1) << "\n";
        dest_report << "  Attribute values (non-null): 0.0 to 500.0\n";
        dest_report << "  Random seed: 33\n\n";
        
        dest_report << "========================================\n";
        dest_report.close();
        
        cout << "  Preview: " << txt_path << endl;
        cout << "  Report: " << dest_report_path << endl;
    }

    // -------------------
    // ACCESSIBILITY TABLE
    // -------------------
    {
        string bin_path = outdir + "/accessibility_" + percent_str + ".bin";
        string txt_path = outdir + "/accessibility_" + percent_str + "_preview.txt";
        string acc_report_path = outdir + "/accessibility_" + percent_str + "_report.txt";
        
        ofstream f_bin(bin_path, ios::binary);
        ofstream f_txt(txt_path);

        // Write header
        f_txt << "origin_id,destination_id,time,distance\n";

        uint64_t total_pairs = 0;

        // Write data
        for (uint32_t o = 0; o < num_origins; ++o) {
            for (uint32_t d = 0; d < num_dests; ++d) {
                Accessibility a;
                a.origin_id = o;
                a.destination_id = d;
                a.time = (o == d) ? 0.0f : frand(1, 120);
                a.distance = (o == d) ? 0.0f : frand(0.5, 50.0);

                f_bin.write((char*)&a, sizeof(a));

                // Only log first 100 lines for preview
                if (total_pairs < 100)
                    f_txt << a.origin_id << "," << a.destination_id << ","
                          << a.time << "," << a.distance << "\n";

                total_pairs++;
            }
        }
        acc_rows = total_pairs;
        uint64_t acc_cols = 4;
        uint64_t acc_bytes_per_row = sizeof(Accessibility);
        acc_bytes = acc_rows * acc_bytes_per_row;
        
        f_bin.close();
        f_txt.close();
        
        cout << "Accessibility file created: " << bin_path << endl;
        cout << "  Rows: " << acc_rows << ", Columns: " << acc_cols
             << ", Bytes/cell: " << (acc_bytes_per_row / acc_cols)
             << ", Total: " << format_size(acc_bytes) << endl;
        
        // Generate accessibility report
        ofstream acc_report(acc_report_path);
        
        acc_report << "========================================\n";
        acc_report << "ACCESSIBILITY TABLE REPORT\n";
        acc_report << "========================================\n\n";
        acc_report << "Dataset percentage: " << percent_int << "%\n";
        acc_report << "Generated: " << __DATE__ << " " << __TIME__ << "\n\n";
        
        acc_report << "FILE INFORMATION:\n";
        acc_report << "  Binary file: accessibility_" << percent_str << ".bin\n";
        acc_report << "  Preview file: accessibility_preview_" << percent_str << ".txt\n";
        acc_report << "  File size: " << format_size(acc_bytes) << " (" << acc_bytes << " bytes)\n\n";
        
        acc_report << "TABLE STRUCTURE:\n";
        acc_report << "  Rows: " << acc_rows << " (full cartesian product)\n";
        acc_report << "  Origins: " << num_origins << "\n";
        acc_report << "  Destinations: " << num_dests << "\n";
        acc_report << "  Columns: 4\n";
        acc_report << "    - origin_id (uint32_t)\n";
        acc_report << "    - destination_id (uint32_t)\n";
        acc_report << "    - time (float)\n";
        acc_report << "    - distance (float)\n";
        acc_report << "  Total cells: " << (acc_rows * 4) << "\n\n";
        
        acc_report << "BINARY FORMAT:\n";
        acc_report << "  Record structure: struct Accessibility {\n";
        acc_report << "    uint32_t origin_id;      // offset 0, 4 bytes\n";
        acc_report << "    uint32_t destination_id; // offset 4, 4 bytes\n";
        acc_report << "    float time;              // offset 8, 4 bytes\n";
        acc_report << "    float distance;          // offset 12, 4 bytes\n";
        acc_report << "  }\n";
        acc_report << "  Bytes per record: " << sizeof(Accessibility) << " bytes\n";
        acc_report << "  Total records: " << acc_rows << "\n";
        acc_report << "  Endianness: Native system endianness\n";
        acc_report << "  No padding between fields\n\n";
        
        acc_report << "DATA ORGANIZATION:\n";
        acc_report << "  Records ordered by: origin_id (outer), destination_id (inner)\n";
        acc_report << "  For each origin: all destinations in sequence\n";
        acc_report << "  No null values (all records present)\n\n";
        
        acc_report << "DATA RANGES:\n";
        acc_report << "  origin_id: 0 to " << (num_origins - 1) << "\n";
        acc_report << "  destination_id: 0 to " << (num_dests - 1) << "\n";
        acc_report << "  time: 0.0 to 120.0 (0.0 when origin == destination)\n";
        acc_report << "  distance: 0.0 to 50.0 (0.0 when origin == destination)\n";
        acc_report << "  Random seed: 33\n\n";
        
        acc_report << "SPECIAL CASES:\n";
        acc_report << "  When origin_id == destination_id:\n";
        acc_report << "    - time = 0.0\n";
        acc_report << "    - distance = 0.0\n\n";
        
        acc_report << "========================================\n";
        acc_report.close();
        
        cout << "  Preview: " << txt_path << endl;
        cout << "  Report: " << acc_report_path << endl;
    }

    cout << "\nAll files created successfully in: " << outdir << endl;
    
    // Print summary
    uint64_t total_bytes = origin_bytes + dest_bytes + acc_bytes;
    cout << "\nSUMMARY:" << endl;
    cout << "  Total data: " << format_size(total_bytes) << endl;
    cout << "  Reports generated: 3" << endl;
}
