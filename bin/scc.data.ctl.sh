#!/usr/bin/env bash

# Function to find virtual environment directory
find_venv() {
    # Common venv directory locations to check
    local locations=(
        "./venv"
        "./.venv"
        "../venv"
        "../.venv"
        "$HOME/.venv"
    )
    
    for dir in "${locations[@]}"; do
        if [ -d "$dir" ] && [ -f "$dir/bin/activate" -o -f "$dir/Scripts/activate" ]; then
            echo "$dir"
            return 0
        fi
    done
    
    return 1
}

# Function to check if we're running in a virtual environment
in_venv() {
    [ -n "$VIRTUAL_ENV" ]
}

# Function to create and activate virtual environment
setup_venv() {
    local venv_dir="$1"
    echo "Creating virtual environment in $venv_dir..."
    python3 -m venv "$venv_dir"
    
    if [ $? -ne 0 ]; then
        echo "Failed to create virtual environment"
        exit 1
    fi
}

# Function to activate virtual environment
activate_venv() {

    echo "===================================================="
    echo "Activating virtual environment..."
    echo "===================================================="
    
    local venv_dir="$1"
    
    if [ -f "$venv_dir/bin/activate" ]; then
        # shellcheck disable=SC1090
        source "$venv_dir/bin/activate"
    elif [ -f "$venv_dir/Scripts/activate" ]; then
        # shellcheck disable=SC1090
        source "$venv_dir/Scripts/activate"
    else
        echo "Error: Cannot find activation script in $venv_dir"
        exit 1
    fi
}

# Main script logic
main() {

    local VENV_DIR
    local REQUIREMENTS_FILE="requirements.txt"

    echo ""
    echo "===================================================="
    echo "Checking for venv"
    echo "===================================================="

    pushd $SCC_DATA_HOME

    # If we're not in a venv, try to find and activate one
    if ! in_venv; then
        echo "No virtual environment currently active"
        
        # Try to find existing venv
        if VENV_DIR=$(find_venv); then
            echo "Found existing virtual environment at: $VENV_DIR"
            activate_venv "$VENV_DIR"
        else
            echo "No existing virtual environment found"
            # Default to ./venv if we need to create one
            VENV_DIR="./venv"
            setup_venv "$VENV_DIR"
            activate_venv "$VENV_DIR"
        fi
        
        # Install requirements if they exist
        if [ -f "$REQUIREMENTS_FILE" ]; then
            echo "Installing requirements..."
            pip install -r "$REQUIREMENTS_FILE"
        fi
        
        echo "Virtual environment is now active: $VIRTUAL_ENV"
        
        # Re-run the script with the same arguments
        exec "$0" "$@"
    else
        echo "Running in virtual environment: $VIRTUAL_ENV"
        # Add your script commands here
        # For example:
        # python your_script.py
        python $SCC_DATA_HOME/src/scc.data.ctl/scc_data_ctl_001/scc_data_ctl.py "$@"
    fi
}

# Run the main function
main "$@"
popd
