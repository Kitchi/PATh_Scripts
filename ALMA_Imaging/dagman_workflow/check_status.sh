#!/bin/bash
#
# Quick status check for ALMA DAGMAN imaging workflow
#

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "========================================"
echo "  DAGMAN Workflow Status"
echo "========================================"
echo ""

# Check if DAG files exist
if [ ! -f "tclean_dag.dag" ]; then
    echo -e "${YELLOW}No DAG file found. Have you run ./run_workflow.sh yet?${NC}"
    exit 0
fi

# Count total jobs in DAG
TOTAL_JOBS=$(grep -c "^JOB" tclean_dag.dag)
echo -e "${BLUE}Total jobs in DAG:${NC} ${TOTAL_JOBS}"
echo ""

# Check if DAG is running
if condor_q -dag 2>/dev/null | grep -q "tclean_dag"; then
    echo -e "${GREEN}DAG is running!${NC}"
    echo ""

    # Show DAG summary
    echo "Job Status Summary:"
    condor_q -dag
    echo ""

    # Count completed jobs from log
    if [ -f "tclean_dag.dag.dagman.out" ]; then
        COMPLETED=$(grep -c "Job.*terminated" tclean_dag.dag.dagman.out 2>/dev/null || echo "0")
        echo -e "${BLUE}Completed jobs:${NC} ${COMPLETED} / ${TOTAL_JOBS}"

        # Calculate percentage
        if [ ${TOTAL_JOBS} -gt 0 ]; then
            PERCENT=$((COMPLETED * 100 / TOTAL_JOBS))
            echo -e "${BLUE}Progress:${NC} ${PERCENT}%"
        fi
        echo ""
    fi

    # Show recent activity
    echo "Recent DAGMan activity (last 10 lines):"
    tail -n 10 tclean_dag.dag.dagman.out 2>/dev/null || echo "No activity log found"

else
    echo -e "${YELLOW}DAG is not currently running${NC}"
    echo ""

    # Check if it completed
    if [ -f "tclean_dag.dag.dagman.out" ]; then
        if grep -q "All jobs Completed" tclean_dag.dag.dagman.out; then
            echo -e "${GREEN}DAG completed successfully!${NC}"
        elif grep -q "ERROR" tclean_dag.dag.dagman.out | tail -5; then
            echo -e "${YELLOW}DAG may have encountered errors. Check:${NC}"
            echo "    tail tclean_dag.dag.dagman.out"
        fi
    fi
fi

echo ""
echo "========================================"
echo "Useful commands:"
echo "  condor_q                           # Check job queue"
echo "  condor_q -dag                      # Check DAG status"
echo "  tail -f tclean_dag.dag.dagman.out  # Monitor DAG log"
echo "  tail -f logs/tclean_*.log          # Monitor job logs"
echo "  condor_rm <cluster_id>             # Cancel DAG"
echo "========================================"
