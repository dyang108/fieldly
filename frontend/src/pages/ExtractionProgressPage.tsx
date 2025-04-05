import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { 
  Box, 
  Paper, 
  Typography, 
  LinearProgress, 
  Button, 
  CircularProgress, 
  Divider, 
  Chip, 
  Alert,
  Card,
  CardContent,
  IconButton,
  Collapse
} from '@mui/material';
import {
  Refresh as RefreshIcon,
  ArrowBack as ArrowBackIcon,
  ExpandMore,
  ExpandLess,
  PlayArrow as PlayArrowIcon,
  Pause as PauseIcon
} from '@mui/icons-material';

interface RouteParams {
  source: string;
  datasetName: string;
}

interface MergeReasoningEntry {
  timestamp: number;
  chunk_index: number;
  total_chunks: number;
  reasoning: Record<string, string>;
  is_final?: boolean;
}

interface ExtractionProgress {
  id: number;
  dataset_name: string;
  source: string;
  status: string;
  message?: string;
  total_files: number;
  processed_files: number;
  current_file?: string;
  file_progress: number;
  total_chunks: number;
  processed_chunks: number;
  current_file_chunks: number;
  current_file_chunk: number;
  files: string[];
  merged_data: any;
  merge_reasoning_history: MergeReasoningEntry[];
  start_time?: string;
  end_time?: string;
  duration?: number;
  is_running?: boolean;
}

const ExtractionProgressPage: React.FC = () => {
  const params = useParams<Record<string, string>>();
  const source = params.source;
  const datasetName = params.datasetName;
  const navigate = useNavigate();
  
  const [loading, setLoading] = useState<boolean>(true);
  const [refreshing, setRefreshing] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<ExtractionProgress | null>(null);
  const [showMergeReasoning, setShowMergeReasoning] = useState<boolean>(true);
  const [resuming, setResuming] = useState<boolean>(false);
  const [pausing, setPausing] = useState<boolean>(false);
  
  // Function to fetch the extraction progress
  const fetchProgress = async () => {
    if (!source || !datasetName) {
      setError('Invalid source or dataset name');
      setLoading(false);
      return;
    }
    
    try {
      setRefreshing(true);
      const response = await fetch(`/api/extraction-progress/dataset/${source}/${encodeURIComponent(datasetName)}`);
      
      if (!response.ok) {
        if (response.status === 404) {
          setError(`No extraction found for dataset ${datasetName}`);
        } else {
          const errorData = await response.json();
          setError(errorData.error || 'Failed to fetch extraction progress');
        }
        setLoading(false);
        setRefreshing(false);
        return;
      }
      
      const data = await response.json();
      setProgress(data.most_recent);
      setError(null);
    } catch (err) {
      setError(`Failed to fetch extraction progress: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  // Check if extraction is currently running
  const checkExtractionStatus = async () => {
    if (!source || !datasetName) return;
    
    try {
      const response = await fetch(`/api/extraction-status/${source}/${encodeURIComponent(datasetName)}`);
      
      if (response.ok) {
        const data = await response.json();
        
        // Update the running status in our progress state
        if (progress) {
          setProgress({
            ...progress,
            is_running: data.is_running
          });
        }
      }
    } catch (err) {
      console.error('Error checking extraction status:', err);
    }
  };

  // Resume a paused extraction
  const resumeExtraction = async () => {
    if (!source || !datasetName || !progress) return;
    
    try {
      setResuming(true);
      const response = await fetch(`/api/extraction-resume/${source}/${encodeURIComponent(datasetName)}`, {
        method: 'POST'
      });
      
      if (response.ok) {
        const data = await response.json();
        if (data.success) {
          // Update the progress with running status
          setProgress({
            ...progress,
            is_running: true,
            status: data.status || 'in_progress',
            message: data.message || 'Extraction resumed'
          });
        } else {
          setError(data.error || 'Failed to resume extraction');
        }
      } else {
        setError('Failed to resume extraction');
      }
    } catch (err) {
      setError(`Error resuming extraction: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setResuming(false);
    }
  };
  
  // Pause a running extraction
  const pauseExtraction = async () => {
    if (!source || !datasetName || !progress) return;
    
    try {
      setPausing(true);
      const response = await fetch(`/api/extraction-pause/${source}/${encodeURIComponent(datasetName)}`, {
        method: 'POST'
      });
      
      if (response.ok) {
        const data = await response.json();
        if (data.success) {
          // Update the progress with paused status
          setProgress({
            ...progress,
            is_running: false,
            status: data.status || 'paused',
            message: data.message || 'Extraction paused'
          });
        } else {
          setError(data.error || 'Failed to pause extraction');
        }
      } else {
        setError('Failed to pause extraction');
      }
    } catch (err) {
      setError(`Error pausing extraction: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setPausing(false);
    }
  };
  
  // Fetch progress on component mount
  useEffect(() => {
    fetchProgress();
    // Also check if extraction is currently running
    checkExtractionStatus();
  }, [source, datasetName]);
  
  // Set up polling to refresh data periodically
  useEffect(() => {
    if (!source || !datasetName) return;
    
    // Refresh more frequently if extraction is running
    const isRunning = progress?.is_running;
    const refreshInterval = isRunning ? 5000 : 15000; // 5 sec if running, 15 sec otherwise
    
    const intervalId = setInterval(() => {
      fetchProgress();
      checkExtractionStatus();
    }, refreshInterval);
    
    return () => {
      clearInterval(intervalId);
    };
  }, [source, datasetName, progress?.is_running]);
  
  // Handle refresh button click
  const handleRefresh = () => {
    fetchProgress();
    checkExtractionStatus();
  };
  
  // Handle back button click
  const handleBack = () => {
    navigate(`/dataset/${source}/${datasetName}`);
  };
  
  // Format a reasoning entry for display
  const formatReasoningEntry = (entry: MergeReasoningEntry) => {
    const timestamp = new Date(entry.timestamp * 1000).toLocaleTimeString();
    const chunkInfo = `Chunk ${entry.chunk_index + 1} of ${entry.total_chunks}`;
    const isFinal = entry.is_final ? " (Final Merge)" : "";
    
    return (
      <Box 
        key={`${entry.timestamp}-${entry.chunk_index}`} 
        sx={{ 
          mb: 2, 
          p: 2, 
          borderRadius: 1, 
          backgroundColor: entry.is_final ? 'rgba(0, 128, 0, 0.1)' : 'rgba(0, 0, 0, 0.03)',
          border: entry.is_final ? '1px solid rgba(0, 128, 0, 0.2)' : '1px solid rgba(0, 0, 0, 0.1)'
        }}
      >
        <Typography variant="subtitle2" gutterBottom>
          {timestamp} - {chunkInfo}{isFinal}
        </Typography>
        <Divider sx={{ my: 1 }} />
        {Object.entries(entry.reasoning).map(([field, reasoning]) => (
          <Box key={field} sx={{ mb: 1 }}>
            <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
              {field}:
            </Typography>
            <Typography variant="body2" sx={{ pl: 2 }}>
              {reasoning}
            </Typography>
          </Box>
        ))}
      </Box>
    );
  };
  
  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '50vh' }}>
        <CircularProgress />
      </Box>
    );
  }
  
  if (error) {
    return (
      <Box sx={{ mt: 3, mx: 'auto', maxWidth: '800px' }}>
        <Button 
          startIcon={<ArrowBackIcon />} 
          onClick={handleBack}
          sx={{ mb: 2 }}
        >
          Back to Dataset
        </Button>
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      </Box>
    );
  }
  
  if (!progress) {
    return (
      <Box sx={{ mt: 3, mx: 'auto', maxWidth: '800px' }}>
        <Button 
          startIcon={<ArrowBackIcon />} 
          onClick={handleBack}
          sx={{ mb: 2 }}
        >
          Back to Dataset
        </Button>
        <Alert severity="info">
          No extraction progress data available for this dataset.
        </Alert>
      </Box>
    );
  }
  
  // Format merged data as string
  const formattedMergedData = Object.keys(progress.merged_data).length > 0
    ? JSON.stringify(progress.merged_data, null, 2)
    : "No merged data available yet";
  
  // Determine if extraction can be resumed
  const canResume = progress.status !== 'completed' && progress.status !== 'failed' && !progress.is_running;
  const canPause = progress.status === 'in_progress' && progress.is_running;
  
  return (
    <Box sx={{ mt: 3, mx: 'auto', maxWidth: '800px' }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Button 
          startIcon={<ArrowBackIcon />} 
          onClick={handleBack}
        >
          Back to Dataset
        </Button>
        <Box>
          {canResume && (
            <Button 
              startIcon={<PlayArrowIcon />} 
              onClick={resumeExtraction}
              disabled={resuming}
              variant="contained"
              color="success"
              sx={{ mr: 1 }}
            >
              {resuming ? 'Resuming...' : 'Resume Extraction'}
            </Button>
          )}
          {canPause && (
            <Button 
              startIcon={<PauseIcon />} 
              onClick={pauseExtraction}
              disabled={pausing}
              variant="contained"
              color="warning"
              sx={{ mr: 1 }}
            >
              {pausing ? 'Pausing...' : 'Pause Extraction'}
            </Button>
          )}
          <Button 
            startIcon={<RefreshIcon />} 
            onClick={handleRefresh}
            disabled={refreshing}
            variant="contained"
          >
            {refreshing ? 'Refreshing...' : 'Refresh Progress'}
          </Button>
        </Box>
      </Box>
      
      <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <Typography variant="h5" gutterBottom>
            Extraction Progress
            <Chip 
              label={progress.status} 
              color={
                progress.status === 'completed' ? 'success' : 
                progress.status === 'failed' ? 'error' : 
                progress.status === 'paused' ? 'warning' :
                progress.status === 'in_progress' ? 'primary' : 'default'
              } 
              size="small" 
              sx={{ ml: 2 }} 
            />
            {progress.is_running && (
              <Chip
                label="Running"
                color="success"
                size="small"
                sx={{ ml: 1 }}
              />
            )}
            {progress.status !== 'completed' && !progress.is_running && (
              <Chip
                label="Paused"
                color="warning"
                size="small"
                sx={{ ml: 1 }}
              />
            )}
          </Typography>
        </Box>
        
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, mb: 2 }}>
          <Box sx={{ flex: '1 1 48%', minWidth: '250px' }}>
            <Card variant="outlined" sx={{ height: '100%' }}>
              <CardContent>
                <Typography variant="subtitle1" gutterBottom>Dataset Info</Typography>
                <Typography variant="body2"><strong>Dataset:</strong> {progress.dataset_name}</Typography>
                <Typography variant="body2"><strong>Source:</strong> {progress.source}</Typography>
                <Typography variant="body2">
                  <strong>Started:</strong> {progress.start_time ? new Date(progress.start_time).toLocaleString() : 'N/A'}
                </Typography>
                {progress.end_time && (
                  <Typography variant="body2">
                    <strong>Completed:</strong> {new Date(progress.end_time).toLocaleString()}
                  </Typography>
                )}
                {progress.duration !== undefined && progress.duration !== null && (
                  <Typography variant="body2">
                    <strong>Duration:</strong> {progress.duration.toFixed(2)}s
                  </Typography>
                )}
              </CardContent>
            </Card>
          </Box>
          
          <Box sx={{ flex: '1 1 48%', minWidth: '250px' }}>
            <Card variant="outlined" sx={{ height: '100%' }}>
              <CardContent>
                <Typography variant="subtitle1" gutterBottom>Progress</Typography>
                <Typography variant="body2">
                  <strong>Files:</strong> {progress.processed_files} / {progress.total_files}
                </Typography>
                <Typography variant="body2">
                  <strong>Current File:</strong> {progress.current_file || 'None'}
                </Typography>
                <Typography variant="body2">
                  <strong>Chunks:</strong> {progress.processed_chunks} / {progress.total_chunks}
                </Typography>
                {progress.message && (
                  <Typography variant="body2" sx={{ mt: 1 }}>
                    <strong>Message:</strong> {progress.message}
                  </Typography>
                )}
              </CardContent>
            </Card>
          </Box>
        </Box>
        
        {progress.status === 'in_progress' && (
          <Box sx={{ mb: 3 }}>
            <Typography variant="subtitle2" gutterBottom>
              File Progress:
            </Typography>
            <LinearProgress 
              variant="determinate"
              value={progress.file_progress * 100}
              sx={{ height: 10, borderRadius: 5 }}
            />
            <Typography variant="caption" color="text.secondary">
              {progress.current_file_chunk} of {progress.current_file_chunks} chunks processed
            </Typography>
          </Box>
        )}
        
        <Divider sx={{ my: 2 }} />
        
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
          <Typography variant="subtitle1">
            Merge Reasoning
          </Typography>
          <Button 
            size="small" 
            variant="text" 
            startIcon={showMergeReasoning ? <ExpandLess /> : <ExpandMore />} 
            onClick={() => setShowMergeReasoning(!showMergeReasoning)}
          >
            {showMergeReasoning ? "Hide" : "Show"}
          </Button>
        </Box>
        
        <Collapse in={showMergeReasoning}>
          <Box 
            sx={{ 
              backgroundColor: 'background.paper', 
              p: 2, 
              borderRadius: 1,
              maxHeight: '300px',
              overflow: 'auto',
              mb: 2
            }}
          >
            {progress.merge_reasoning_history && progress.merge_reasoning_history.length > 0 ? (
              progress.merge_reasoning_history.map(entry => formatReasoningEntry(entry))
            ) : (
              <Typography variant="body2" color="text.secondary">
                No merge reasoning available yet.
              </Typography>
            )}
          </Box>
        </Collapse>
        
        <Typography variant="subtitle1" gutterBottom>
          Merged Data Preview
        </Typography>
        <Box 
          sx={{ 
            backgroundColor: 'background.paper', 
            p: 2, 
            borderRadius: 1,
            maxHeight: '400px',
            overflow: 'auto',
            whiteSpace: 'pre-wrap',
            fontFamily: 'monospace',
            fontSize: '0.8rem'
          }}
        >
          {formattedMergedData}
        </Box>
      </Paper>
    </Box>
  );
};

export default ExtractionProgressPage; 