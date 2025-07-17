#!/usr/bin/env python3
"""
Test framework for FoundationDB that integrates with existing bash fixtures.

This provides a Python interface to the shared bash infrastructure while
maintaining compatibility with existing test scripts.
"""

import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json
import time

logger = logging.getLogger(__name__)


class BashFixture:
    """Base class for wrapping bash fixture scripts."""
    
    def __init__(self, script_path: str, test_scratch_dir: Path):
        self.script_path = Path(script_path)
        self.test_scratch_dir = test_scratch_dir
        self._sourced_functions = {}
        
    def _run_bash_function(self, function_name: str, *args) -> subprocess.CompletedProcess:
        """Run a bash function from the fixture script."""
        script = f'''
        set -euo pipefail
        source "{self.script_path}"
        {function_name} {' '.join(f'"{arg}"' for arg in args)}
        '''
        
        return subprocess.run(
            ['bash', '-c', script],
            capture_output=True,
            text=True,
            cwd=str(self.test_scratch_dir)
        )
    
    def _run_bash_function_with_output(self, function_name: str, *args) -> Tuple[bool, str]:
        """Run bash function and return success status and output."""
        result = self._run_bash_function(function_name, *args)
        return result.returncode == 0, result.stdout.strip()


class TestsCommon(BashFixture):
    """Python wrapper for tests_common.sh functionality."""
    
    def __init__(self, test_scratch_dir: Path):
        script_path = Path(__file__).parent.parent / "tests_common.sh"
        super().__init__(script_path, test_scratch_dir)
    
    def log(self, message: str):
        """Log a message with timestamp."""
        success, output = self._run_bash_function_with_output("log", message)
        if success:
            print(output)
        else:
            logger.error(f"Failed to log message: {message}")
    
    def err(self, message: str):
        """Log an error message."""
        success, output = self._run_bash_function_with_output("err", message)
        if success:
            print(output, file=sys.stderr)
        else:
            logger.error(f"Failed to log error: {message}")
    
    def load_data(self, build_dir: str, scratch_dir: str) -> bool:
        """Load test data into FDB."""
        result = self._run_bash_function("load_data", build_dir, scratch_dir)
        return result.returncode == 0
    
    def verify_data(self, build_dir: str, scratch_dir: str) -> bool:
        """Verify test data in FDB."""
        result = self._run_bash_function("verify_data", build_dir, scratch_dir)
        return result.returncode == 0
    
    def clear_data(self, build_dir: str, scratch_dir: str) -> bool:
        """Clear test data from FDB."""
        result = self._run_bash_function("clear_data", build_dir, scratch_dir)
        return result.returncode == 0
    
    def has_data(self, build_dir: str, scratch_dir: str) -> bool:
        """Check if FDB has test data."""
        result = self._run_bash_function("has_data", build_dir, scratch_dir)
        return result.returncode == 0
    
    def log_test_result(self, test_errcode: int, test_name: str):
        """Log test pass/fail result."""
        self._run_bash_function("log_test_result", str(test_errcode), test_name)
    
    def grep_for_severity40(self, directory: str) -> bool:
        """Check for severity 40 errors in logs."""
        result = self._run_bash_function("grep_for_severity40", directory)
        return result.returncode == 0


class FDBClusterFixture(BashFixture):
    """Python wrapper for fdb_cluster_fixture.sh functionality."""
    
    def __init__(self, test_scratch_dir: Path):
        script_path = Path(__file__).parent.parent / "fdb_cluster_fixture.sh"
        super().__init__(script_path, test_scratch_dir)
        self.cluster_running = False
    
    def start_fdb_cluster(self, source_dir: str, build_dir: str, 
                         scratch_dir: str, ss_count: int = 1, 
                         knobs: List[str] = None) -> bool:
        """Start an FDB cluster."""
        args = [source_dir, build_dir, scratch_dir, str(ss_count)]
        if knobs:
            args.extend(knobs)
        
        result = self._run_bash_function("start_fdb_cluster", *args)
        self.cluster_running = result.returncode == 0
        return self.cluster_running
    
    def shutdown_fdb_cluster(self):
        """Shutdown the FDB cluster."""
        if self.cluster_running:
            self._run_bash_function("shutdown_fdb_cluster")
            self.cluster_running = False
    
    def start_backup_agent(self, build_dir: str, scratch_dir: str) -> bool:
        """Start the backup agent."""
        result = self._run_bash_function("start_backup_agent", build_dir, scratch_dir)
        return result.returncode == 0
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown_fdb_cluster()


class AWSFixture(BashFixture):
    """Python wrapper for aws_fixture.sh functionality."""
    
    def __init__(self, test_scratch_dir: Path):
        script_path = Path(__file__).parent.parent / "aws_fixture.sh"
        super().__init__(script_path, test_scratch_dir)
    
    def create_aws_dir(self, scratch_dir: str) -> Optional[str]:
        """Create AWS test directory."""
        success, output = self._run_bash_function_with_output("create_aws_dir", scratch_dir)
        return output if success else None
    
    def aws_setup(self, build_dir: str, test_scratch_dir: str) -> Optional[List[str]]:
        """Setup AWS configuration."""
        success, output = self._run_bash_function_with_output("aws_setup", build_dir, test_scratch_dir)
        if success:
            # Parse the output - aws_setup returns multiple lines
            return output.strip().split('\n')
        return None
    
    def shutdown_aws(self, test_scratch_dir: str):
        """Shutdown AWS resources."""
        self._run_bash_function("shutdown_aws", test_scratch_dir)


class SeaweedFSFixture(BashFixture):
    """Python wrapper for seaweedfs_fixture.sh functionality."""
    
    def __init__(self, test_scratch_dir: Path):
        script_path = Path(__file__).parent.parent / "seaweedfs_fixture.sh"
        super().__init__(script_path, test_scratch_dir)
        self.seaweed_running = False
    
    def create_weed_dir(self, scratch_dir: str) -> Optional[str]:
        """Create SeaweedFS test directory."""
        success, output = self._run_bash_function_with_output("create_weed_dir", scratch_dir)
        return output if success else None
    
    def run_weed(self, scratch_dir: str, test_scratch_dir: str) -> Optional[str]:
        """Start SeaweedFS server."""
        success, output = self._run_bash_function_with_output("run_weed", scratch_dir, test_scratch_dir)
        if success:
            self.seaweed_running = True
            return output  # Returns the host:port
        return None
    
    def shutdown_weed(self, test_scratch_dir: str):
        """Shutdown SeaweedFS server."""
        if self.seaweed_running:
            self._run_bash_function("shutdown_weed", test_scratch_dir)
            self.seaweed_running = False
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown_weed(str(self.test_scratch_dir))


class FDBTestFramework:
    """Main test framework that coordinates all fixtures."""
    
    def __init__(self, build_dir: str, scratch_dir: str = None):
        self.build_dir = Path(build_dir)
        self.scratch_dir = Path(scratch_dir) if scratch_dir else Path(tempfile.gettempdir())
        self.test_scratch_dir = self.scratch_dir / f"fdb_test_{int(time.time())}"
        self.test_scratch_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize fixtures
        self.common = TestsCommon(self.test_scratch_dir)
        self.fdb_cluster = FDBClusterFixture(self.test_scratch_dir)
        self.aws = AWSFixture(self.test_scratch_dir)
        self.seaweedfs = SeaweedFSFixture(self.test_scratch_dir)
        
        # Test tracking
        self.test_results = []
    
    def run_test(self, test_name: str, test_func, *args, **kwargs):
        """Run a single test and track results."""
        logger.info(f"Running test: {test_name}")
        try:
            test_func(*args, **kwargs)
            self.test_results.append((test_name, True, None))
            self.common.log_test_result(0, test_name)
        except Exception as e:
            self.test_results.append((test_name, False, str(e)))
            self.common.log_test_result(1, test_name)
            logger.error(f"Test {test_name} failed: {e}")
    
    def cleanup(self):
        """Clean up all resources."""
        self.fdb_cluster.shutdown_fdb_cluster()
        self.seaweedfs.shutdown_weed(str(self.test_scratch_dir))
        self.aws.shutdown_aws(str(self.test_scratch_dir))
        
        # Clean up test directory
        import shutil
        if self.test_scratch_dir.exists():
            shutil.rmtree(self.test_scratch_dir)
    
    def print_results(self) -> bool:
        """Print test results summary."""
        passed = sum(1 for _, success, _ in self.test_results if success)
        total = len(self.test_results)
        
        print(f"\nTest Results: {passed}/{total} passed")
        
        for test_name, success, error in self.test_results:
            status = "✓" if success else "✗"
            print(f"{status} {test_name}")
            if error:
                print(f"  Error: {error}")
        
        return passed == total
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup() 