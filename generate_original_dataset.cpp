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

    srand(42);
    filesystem::create_directories(outdir);

    // Calculate scaled sizes
    uint32_t num_origins = static_cast<uint32_t>(NUM_ORIGINS * percent);
    uint32_t num_dests   = static_cast<uint32_t>(NUM_DESTS * percent);

    // Generate null percentages for each attribute column
    vector<float> origin_null_perc(ORIGIN_ATTRS);
    vector<float> dest_null_perc(DEST_ATTRS);
    for (uint32_t a = 0; a < ORIGIN_ATTRS; ++a)
        origin_null_perc[a] = frand(0.6f, 0.6f);
    for (uint32_t a = 0; a < DEST_ATTRS; ++a)
        dest_null_perc[a] = frand(0.6f, 0.6f);

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

    // -------------------
    // ORIGIN TABLE
    // -------------------
    {
        string bin_path = outdir + "/origin_" + percent_str + ".bin";
        string txt_path = outdir + "/origin_preview_" + percent_str + ".txt";
        ofstream f_bin(bin_path, ios::binary);
        ofstream f_txt(txt_path);

        // Write header
        f_txt << "id";
        for (uint32_t a = 1; a <= ORIGIN_ATTRS; ++a)
            f_txt << ",att" << a;
        f_txt << "\n";

        // Write data
        for (uint32_t id = 0; id < num_origins; ++id) {
            f_bin.write((char*)&id, 4);
            if (id < 100) f_txt << id;
            for (uint32_t a = 0; a < ORIGIN_ATTRS; ++a) {
                float val;
                bool is_null = (frand() < origin_null_perc[a]);
                if (is_null) {
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
        uint64_t origin_cells = uint64_t(num_origins) * (1 + ORIGIN_ATTRS);
        uint64_t origin_bytes = origin_cells * 4;
        cout << "Origin file created: " << bin_path << endl;
        cout << "  Rows: " << num_origins << ", Columns: " << (1 + ORIGIN_ATTRS)
             << ", Bytes/cell: 4, Total: " << format_size(origin_bytes) << endl;
    }

    // -------------------
    // DESTINATION TABLE
    // -------------------
    {
        string bin_path = outdir + "/destination_" + percent_str + ".bin";
        string txt_path = outdir + "/destination_preview_" + percent_str + ".txt";
        ofstream f_bin(bin_path, ios::binary);
        ofstream f_txt(txt_path);

        // Write header
        f_txt << "id";
        for (uint32_t a = 1; a <= DEST_ATTRS; ++a)
            f_txt << ",att" << a;
        f_txt << "\n";

        // Write data
        for (uint32_t id = 0; id < num_dests; ++id) {
            f_bin.write((char*)&id, 4);
            if (id < 100) f_txt << id;
            for (uint32_t a = 0; a < DEST_ATTRS; ++a) {
                float val;
                bool is_null = (frand() < dest_null_perc[a]);
                if (is_null) {
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
        uint64_t dest_cells = uint64_t(num_dests) * (1 + DEST_ATTRS);
        uint64_t dest_bytes = dest_cells * 4;
        cout << "Destination file created: " << bin_path << endl;
        cout << "  Rows: " << num_dests << ", Columns: " << (1 + DEST_ATTRS)
             << ", Bytes/cell: 4, Total: " << format_size(dest_bytes) << endl;
    }

    // -------------------
    // ACCESSIBILITY TABLE
    // -------------------
    {
        string bin_path = outdir + "/accessibility_" + percent_str + ".bin";
        string txt_path = outdir + "/accessibility_preview_" + percent_str + ".txt";
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
        uint64_t acc_rows = total_pairs;
        uint64_t acc_cols = 4;
        uint64_t acc_bytes_per_row = sizeof(Accessibility);
        uint64_t acc_bytes = acc_rows * acc_bytes_per_row;
        cout << "Accessibility file created: " << bin_path << endl;
        cout << "  Rows: " << acc_rows << ", Columns: " << acc_cols
             << ", Bytes/cell: " << (acc_bytes_per_row / acc_cols)
             << ", Total: " << format_size(acc_bytes) << endl;
    }

    cout << "\nAll files created successfully in: " << outdir << endl;
}
