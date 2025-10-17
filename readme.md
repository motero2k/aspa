# ASPA Dataset Generation and Processing

## 1. Generate Original Dataset

```sh
g++ -O3 -std=c++17 generate_original_dataset.cpp -o generate_original_dataset
./generate_original_dataset dataset_raw 0.1
```

This generates files with suffix `10p` (for 0.1 = 10%).

## 2. Preprocess Dataset

```sh
g++ -O3 -std=c++17 preprocess_dataset.cpp -o preprocess_dataset
./preprocess_dataset dataset_raw 0.1
```

This processes the `10p` files and creates output in `out/10p/` directory.

## 3. Query Filter

```sh
g++ -O3 -std=c++17 query_filter.cpp -o query_filter
./query_filter 10 att3 att7 result_10p.txt
```

**Note:** The query filter now takes the percentage as an integer (e.g., `10` for 10%, `5` for 5%). It will look for data in `out/10p/` directory.

## Examples for different percentages:

- For 5% dataset: `./query_filter 5 att3 att7 result_5p.txt`
- For 10% dataset: `./query_filter 10 att3 att7 result_10p.txt`
- For 50% dataset: `./query_filter 50 att3 att7 result_50p.txt`
