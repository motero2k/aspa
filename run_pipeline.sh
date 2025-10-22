#!/bin/bash
# filepath: /home/motero/isa/aspa/run_pipeline.sh

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
INPUT_CSV="report.input.csv"
RAW_DATA_DIR="dataset_raw"
PROCESSED_DATA_DIR="dataset_processed"
RESULTS_DIR="results"
LOGS_DIR="logs"
SLEEP_DELAY=${1:-5}  # Default 5 seconds, can be overridden by first argument

# Create directories
mkdir -p "$RAW_DATA_DIR" "$PROCESSED_DATA_DIR" "$RESULTS_DIR" "$LOGS_DIR"

# Track which datasets have been generated and processed
declare -A DATASETS_GENERATED
declare -A DATASETS_PREPROCESSED

# Function to print colored log messages
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to compile programs if needed
compile_programs() {
    log_info "Checking if programs need compilation..."
    
    if [ ! -f "generate_original_dataset" ] || [ "generate_original_dataset.cpp" -nt "generate_original_dataset" ]; then
        log_info "Compiling generate_original_dataset..."
        g++ -O3 -std=c++17 generate_original_dataset.cpp -o generate_original_dataset
        log_success "generate_original_dataset compiled"
    fi
    
    if [ ! -f "preprocess_dataset" ] || [ "preprocess_dataset.cpp" -nt "preprocess_dataset" ]; then
        log_info "Compiling preprocess_dataset..."
        g++ -O3 -std=c++17 preprocess_dataset.cpp -o preprocess_dataset
        log_success "preprocess_dataset compiled"
    fi
    
    if [ ! -f "query_filter" ] || [ "query_filter.cpp" -nt "query_filter" ]; then
        log_info "Compiling query_filter..."
        g++ -O3 -std=c++17 query_filter.cpp -o query_filter
        log_success "query_filter compiled"
    fi
}

# Function to check if dataset exists
dataset_exists() {
    local percent=$1
    local suffix="${percent}p"
    
    if [ -f "${RAW_DATA_DIR}/origin_${suffix}.bin" ] && \
       [ -f "${RAW_DATA_DIR}/destination_${suffix}.bin" ] && \
       [ -f "${RAW_DATA_DIR}/accessibility_${suffix}.bin" ]; then
        return 0
    else
        return 1
    fi
}

# Function to check if preprocessed dataset exists
preprocessed_exists() {
    local percent=$1
    local suffix="${percent}p"
    
    if [ -d "${PROCESSED_DATA_DIR}/${suffix}/attributes/origin" ] && \
       [ -d "${PROCESSED_DATA_DIR}/${suffix}/attributes/destination" ] && \
       [ -d "${PROCESSED_DATA_DIR}/${suffix}/accessibility" ]; then
        return 0
    else
        return 1
    fi
}

# Function to generate dataset
generate_dataset() {
    local percent=$1
    local percent_float=$(awk "BEGIN {print $percent/100}")
    local log_file="${LOGS_DIR}/generate_${percent}p.log"
    
    log_info "Generating dataset for ${percent}%..."
    
    if ./generate_original_dataset "$RAW_DATA_DIR" "$percent_float" > "$log_file" 2>&1; then
        log_success "Dataset ${percent}% generated successfully"
        DATASETS_GENERATED[$percent]=1
        return 0
    else
        log_error "Failed to generate dataset ${percent}%. Check log: $log_file"
        return 1
    fi
}

# Function to preprocess dataset
preprocess_dataset() {
    local percent=$1
    local percent_float=$(awk "BEGIN {print $percent/100}")
    local log_file="${LOGS_DIR}/preprocess_${percent}p.log"
    
    log_info "Preprocessing dataset for ${percent}%..."
    
    if ./preprocess_dataset "$RAW_DATA_DIR" "$percent_float" "$PROCESSED_DATA_DIR" > "$log_file" 2>&1; then
        log_success "Dataset ${percent}% preprocessed successfully"
        DATASETS_PREPROCESSED[$percent]=1
        return 0
    else
        log_error "Failed to preprocess dataset ${percent}%. Check log: $log_file"
        return 1
    fi
}

# Function to run query
run_query() {
    local percent=$1
    local origin_attr=$2
    local dest_attr=$3
    local percent_float=$(awk "BEGIN {print $percent/100}")
    local log_file="${LOGS_DIR}/query_${percent}p_${origin_attr}_${dest_attr}.log"
    
    log_info "Running query: ${percent}%, origin=att${origin_attr}, dest=att${dest_attr}..."
    
    if ./query_filter "$PROCESSED_DATA_DIR" "$percent_float" "att${origin_attr}" "att${dest_attr}" "$RESULTS_DIR" > "$log_file" 2>&1; then
        log_success "Query completed: ${percent}%, att${origin_attr}, att${dest_attr}"
        return 0
    else
        log_error "Query failed: ${percent}%, att${origin_attr}, att${dest_attr}. Check log: $log_file"
        return 1
    fi
}

# Main execution
main() {
    log_info "========================================="
    log_info "ASPA Pipeline Execution Started"
    log_info "========================================="
    log_info "Input file: $INPUT_CSV"
    log_info "Sleep delay between iterations: ${SLEEP_DELAY} seconds"
    log_info ""
    
    # Compile programs
    compile_programs
    log_info ""
    
    # Check if input file exists
    if [ ! -f "$INPUT_CSV" ]; then
        log_error "Input file not found: $INPUT_CSV"
        exit 1
    fi
    
    # Read CSV and process each line
    local line_count=0
    local total_lines=$(tail -n +2 "$INPUT_CSV" | wc -l)
    
    log_info "Total configurations to process: $total_lines"
    log_info ""
    
    # Skip header and process each line
    tail -n +2 "$INPUT_CSV" | while IFS=',' read -r percent origin dest; do
        line_count=$((line_count + 1))
        
        log_info "========================================="
        log_info "Processing configuration $line_count/$total_lines"
        log_info "Dataset: ${percent}%, Origin: att${origin}, Dest: att${dest}"
        log_info "========================================="
        
        # Step 1: Generate dataset if not already generated
        if dataset_exists "$percent"; then
            log_warning "Dataset ${percent}% already exists, skipping generation"
        else
            if [ -z "${DATASETS_GENERATED[$percent]}" ]; then
                generate_dataset "$percent" || continue
            else
                log_warning "Dataset ${percent}% was generated in this run"
            fi
        fi
        
        # Step 2: Preprocess dataset if not already preprocessed
        if preprocessed_exists "$percent"; then
            log_warning "Preprocessed dataset ${percent}% already exists, skipping preprocessing"
        else
            if [ -z "${DATASETS_PREPROCESSED[$percent]}" ]; then
                preprocess_dataset "$percent" || continue
            else
                log_warning "Dataset ${percent}% was preprocessed in this run"
            fi
        fi
        
        # Step 3: Run query
        run_query "$percent" "$origin" "$dest"
        
        # Step 4: Wait to stabilize system before next iteration
        if [ $line_count -lt $total_lines ]; then
            log_info "Waiting ${SLEEP_DELAY} seconds to stabilize system before next iteration..."
            sleep "$SLEEP_DELAY"
        fi
        
        log_info ""
    done
    
    log_success "========================================="
    log_success "Pipeline execution completed!"
    log_success "========================================="
    log_info "Raw datasets: $RAW_DATA_DIR"
    log_info "Preprocessed datasets: $PROCESSED_DATA_DIR"
    log_info "Query results: $RESULTS_DIR"
    log_info "Logs: $LOGS_DIR"
    log_info "Combined report: report.csv"
}

# Run main function
main