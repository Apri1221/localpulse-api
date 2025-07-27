#!/bin/bash
# LocalPulse Claude LLM Server Background Startup Script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDFILE="$SCRIPT_DIR/localpulse.pid"
LOGFILE="$SCRIPT_DIR/localpulse.log"

echo "🚀 Starting LocalPulse with Claude LLM..."

# Function to start server in background
start_server() {
    echo "🚀 Starting LocalPulse with Claude LLM in background..."
    
    # Function to safely kill processes
    kill_related_processes() {
        echo "🔍 Checking for existing related processes..."
        
        # Find and kill processes related to our server
        local processes_found=false
        
        # Kill Python processes running api.py
        if pgrep -f "python.*api.py" > /dev/null; then
            echo "🔴 Killing existing api.py processes..."
            pkill -f "python.*api.py"
            processes_found=true
        fi
        
        # Kill Python processes running simple_api.py
        if pgrep -f "python.*simple_api.py" > /dev/null; then
            echo "🔴 Killing existing simple_api.py processes..."
            pkill -f "python.*simple_api.py"
            processes_found=true
        fi
        
        # Kill any Flask processes on port 8081
        if lsof -i :8081 > /dev/null 2>&1; then
            echo "🔴 Killing processes using port 8081..."
            lsof -ti :8081 | xargs kill -9 2>/dev/null || true
            processes_found=true
        fi
        
        # Kill any Flask processes on port 5000 (default Flask port)
        if lsof -i :5000 > /dev/null 2>&1; then
            echo "🔴 Killing processes using port 5000..."
            lsof -ti :5000 | xargs kill -9 2>/dev/null || true
            processes_found=true
        fi
        
        # Kill any processes with 'localpulse' in command line
        if pgrep -f "localpulse" > /dev/null; then
            echo "🔴 Killing other LocalPulse related processes..."
            pkill -f "localpulse"
            processes_found=true
        fi
        
        if [ "$processes_found" = true ]; then
            echo "⏳ Waiting for processes to terminate..."
            sleep 2
            
            # Force kill any remaining processes
            pkill -9 -f "python.*api.py" 2>/dev/null || true
            pkill -9 -f "python.*simple_api.py" 2>/dev/null || true
            
            echo "✅ Cleanup completed"
        else
            echo "✅ No existing processes found"
        fi
    }

    # Function to verify port is free
    check_port_availability() {
        if lsof -i :8081 > /dev/null 2>&1; then
            echo "❌ Port 8081 is still in use. Attempting force cleanup..."
            lsof -ti :8081 | xargs kill -9 2>/dev/null || true
            sleep 1
            
            if lsof -i :8081 > /dev/null 2>&1; then
                echo "❌ ERROR: Unable to free port 8081. Manual intervention required."
                echo "Run: lsof -i :8081 to see what's using the port"
                exit 1
            fi
        fi
        echo "✅ Port 8081 is available"
    }

    # Kill related processes
    kill_related_processes

    # Verify port is free
    check_port_availability

    # Check if virtual environment is activated
    if [[ "$VIRTUAL_ENV" == "" ]]; then
        echo "🔧 Activating virtual environment..."
        if [ -d "$SCRIPT_DIR/venv" ]; then
            source "$SCRIPT_DIR/venv/bin/activate"
            echo "✅ Virtual environment activated"
        else
            echo "⚠️ Virtual environment not found at $SCRIPT_DIR/venv/"
            echo "💡 Create it with: python -m venv venv"
        fi
    fi

    # Load environment variables
    if [ -f "$SCRIPT_DIR/.env" ]; then
        echo "🔧 Loading environment variables from .env..."
        export $(cat "$SCRIPT_DIR/.env" | grep -v '^#' | xargs)
        echo "✅ Environment variables loaded"
    else
        echo "💡 No .env file found. Create one with your CLAUDE_API_KEY if needed."
    fi

    # Check Claude API key
    if [ -z "$CLAUDE_API_KEY" ]; then
        echo "⚠️ CLAUDE_API_KEY not set. Claude features will be disabled."
        echo "💡 Set your API key: export CLAUDE_API_KEY=your_key_here"
        echo "💡 Or add it to .env file: echo 'CLAUDE_API_KEY=your_key_here' > .env"
    else
        echo "✅ Claude API key is configured"
    fi

    # Check if required files exist
    if [ ! -f "$SCRIPT_DIR/api.py" ]; then
        echo "❌ ERROR: api.py not found in project directory"
        echo "💡 Make sure you're running this script from the project root"
        exit 1
    fi

    if [ ! -f "$SCRIPT_DIR/localpulse.db" ]; then
        echo "⚠️ Database not found. Creating minimal database..."
        if [ -f "$SCRIPT_DIR/recreate_database.py" ]; then
            cd "$SCRIPT_DIR" && python recreate_database.py
        else
            echo "❌ recreate_database.py not found. Database setup required."
        fi
    fi

    # Start server in background
    echo "🚀 Starting server on http://localhost:8081 in background..."
    echo "📋 Logs will be written to: $LOGFILE"
    
    cd "$SCRIPT_DIR"
    nohup python api.py > "$LOGFILE" 2>&1 &
    SERVER_PID=$!
    
    # Save PID to file
    echo $SERVER_PID > "$PIDFILE"
    
    # Wait a moment to check if server started successfully
    sleep 2
    if kill -0 $SERVER_PID 2>/dev/null; then
        echo "✅ Server started successfully with PID: $SERVER_PID"
        echo "📋 View logs: tail -f $LOGFILE"
        echo "🛑 Stop server: $0 stop"
    else
        echo "❌ Server failed to start. Check logs: $LOGFILE"
        exit 1
    fi
}

# Function to stop server
stop_server() {
    if [ -f "$PIDFILE" ]; then
        PID=$(cat "$PIDFILE")
        if kill -0 $PID 2>/dev/null; then
            echo "🛑 Stopping LocalPulse server (PID: $PID)..."
            kill $PID
            sleep 2
            
            # Force kill if still running
            if kill -0 $PID 2>/dev/null; then
                echo "🔴 Force stopping server..."
                kill -9 $PID
            fi
            
            rm -f "$PIDFILE"
            echo "✅ Server stopped"
        else
            echo "⚠️ Server not running (stale PID file)"
            rm -f "$PIDFILE"
        fi
    else
        echo "⚠️ No PID file found. Attempting to kill related processes..."
        pkill -f "python.*api.py" && echo "✅ Killed api.py processes" || echo "ℹ️ No api.py processes found"
    fi
}

# Function to check server status
status_server() {
    if [ -f "$PIDFILE" ]; then
        PID=$(cat "$PIDFILE")
        if kill -0 $PID 2>/dev/null; then
            echo "✅ LocalPulse server is running (PID: $PID)"
            echo "🌐 URL: http://localhost:8081"
            echo "📋 Logs: tail -f $LOGFILE"
        else
            echo "❌ Server not running (stale PID file)"
            rm -f "$PIDFILE"
        fi
    else
        echo "❌ Server not running"
    fi
}

# Function to show logs
show_logs() {
    if [ -f "$LOGFILE" ]; then
        tail -f "$LOGFILE"
    else
        echo "❌ Log file not found: $LOGFILE"
    fi
}

# Main
case "$1" in
    start)
        start_server
        ;;
    stop)
        stop_server
        ;;
    restart)
        stop_server
        sleep 1
        start_server
        ;;
    status)
        status_server
        ;;
    logs)
        show_logs
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs}"
        echo ""
        echo "Commands:"
        echo "  start   - Start the server in background"
        echo "  stop    - Stop the server"
        echo "  restart - Restart the server"
        echo "  status  - Check server status"
        echo "  logs    - Show server logs (Ctrl+C to exit)"
        exit 1
        ;;
esac