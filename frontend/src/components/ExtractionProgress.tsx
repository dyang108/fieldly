import React, { useEffect, useState, useCallback, useRef } from 'react';
import { 
  Box, 
  Paper, 
  Typography, 
  LinearProgress, 
  Chip,
  Divider,
  CircularProgress,
  Alert,
  IconButton,
  Collapse,
  Button
} from '@mui/material';
import { 
  ExpandMore, 
  ExpandLess, 
  Refresh as RefreshIcon,
  WifiOff as WifiOffIcon,
  Visibility as VisibilityIcon
} from '@mui/icons-material';
import { 
  getSocket, 
  joinRoom, 
  leaveRoom,
  isSocketConnected 
} from '../utils/socket';

interface ExtractionProgressProps {
  source: string;
  datasetName: string;
  onComplete?: () => void;
  initialMode?: 'active' | 'check';
}

interface ExtractionState {
  status: string;
  total_files: number;
  processed_files: number;
  current_file: string;
  file_progress: number;
  merged_data: any;
  files: string[];
  start_time?: number;
  end_time?: number;
  duration?: number;
  message?: string;
  total_chunks: number;
  processed_chunks: number;
  current_file_chunks: number;
  current_file_chunk: number;
}

interface SimplifiedMergedData {
  dataSize: number;
  keys: string[];
  status: string;
}

const ExtractionProgress: React.FC<ExtractionProgressProps> = ({ 
  source, 
  datasetName, 
  onComplete, 
  initialMode = 'check' 
}) => {
  // Core state
  const [state, setState] = useState<ExtractionState>({
    status: 'idle',
    total_files: 0,
    processed_files: 0,
    current_file: '',
    file_progress: 0,
    merged_data: {},
    files: [],
    total_chunks: 0,
    processed_chunks: 0,
    current_file_chunks: 0,
    current_file_chunk: 0
  });
  const [error, setError] = useState<string>('');
  const [connected, setConnected] = useState<boolean>(false);
  const [simplifiedData, setSimplifiedData] = useState<SimplifiedMergedData | null>(null);
  const [showDetails, setShowDetails] = useState<boolean>(false);
  const [checkingStatus, setCheckingStatus] = useState<boolean>(false);
  const [activeSession, setActiveSession] = useState<boolean>(initialMode === 'active');
  const [receivedData, setReceivedData] = useState<boolean>(false);
  
  // References
  const isMounted = useRef(true);
  const socket = useRef<any>(null);
  const reconnectTimerRef = useRef<any>(null);
  const roomId = `${source}_${datasetName}`;

  // Debug function to trace state updates
  const debugStateUpdate = (source: string, data: any) => {
    console.log(`[Socket Event] ${source}:`, data);
    
    // Log the current state before update
    console.log(`Current state before update:`, {
      status: state.status,
      processed_files: state.processed_files,
      total_files: state.total_files,
      file_progress: state.file_progress
    });
    
    // If we received any data from socket but still show as disconnected,
    // update our connection status
    if (!connected) {
      console.log('Received socket data while disconnected - updating connection status');
      setConnected(true);
      setReceivedData(true);
    }
  };

  // Effect to check for real connection status
  useEffect(() => {
    // If we received data but show disconnected, fix the connection state
    if (receivedData && !connected && socket.current) {
      console.log('Received socket data while showing disconnected - fixing connection state');
      setConnected(true);
    }
  }, [receivedData, connected]);

  // Check if there's an active extraction session
  const checkExtractionSession = useCallback(async () => {
    if (!source || !datasetName) return;
    
    setCheckingStatus(true);
    setError('');
    
    try {
      const response = await fetch(`http://localhost:5000/api/extraction-room-status/${source}/${encodeURIComponent(datasetName)}`);
      const data = await response.json();
      
      if (data.active) {
        console.log(`ExtractionProgress: Found active session for ${roomId}`, data);
        setActiveSession(true);
      } else {
        console.log(`ExtractionProgress: No active session for ${roomId}`, data);
        setActiveSession(false);
        setError('No active extraction session found for this dataset.');
      }
    } catch (error) {
      console.error(`ExtractionProgress: Error checking session status:`, error);
      setError('Failed to check extraction status. Please try again.');
      setActiveSession(false);
    } finally {
      setCheckingStatus(false);
    }
  }, [source, datasetName, roomId]);

  // Poll for connection status to ensure UI matches reality
  useEffect(() => {
    if (!activeSession || !source || !datasetName) return;
    
    const checkConnectionStatus = () => {
      const isConnected = isSocketConnected();
      const socketExists = !!socket.current;
      
      if (isConnected && socketExists && !connected) {
        console.log('Connection check: Socket is connected but UI shows disconnected - updating state');
        setConnected(true);
        
        // Make sure we're in the right room
        joinRoom(source, datasetName);
        
        // Fetch current state to make sure we have the latest data
        fetchCurrentState();
      }
    };
    
    // Check immediately on mount
    checkConnectionStatus();
    
    // Then check every few seconds
    const intervalId = setInterval(checkConnectionStatus, 3000);
    
    return () => {
      clearInterval(intervalId);
    };
  }, [activeSession, source, datasetName, connected]);

  // Connect to socket for the extraction session
  useEffect(() => {
    if (!activeSession || !source || !datasetName) return;

    console.log(`[Socket] Setting up socket connection for ${roomId}`);
    
    // Clear any previous reconnect timers
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
    
    // Get socket instance
    socket.current = getSocket();
    
    if (!socket.current) {
      console.error('[Socket] Unable to get socket instance');
      setError('Could not connect to extraction service');
      return;
    }

    // Setup event handlers
    const handleConnect = () => {
      if (!isMounted.current) return;
      console.log(`[Socket] Connected for ${roomId}`);
      setConnected(true);
      setError('');

      // Join room and request current status
      console.log(`[Socket] Joining room ${roomId}`);
      joinRoom(source, datasetName);
      
      // Also fetch current state to make sure we're in sync
      console.log('[Socket] Fetching current state to ensure sync');
      fetchCurrentState();
    };
    
    const handleDisconnect = (reason: string) => {
      if (!isMounted.current) return;
      console.log(`[Socket] Disconnected: ${reason}`);
      setConnected(false);
    };
    
    const handleError = (err: any) => {
      if (!isMounted.current) return;
      console.error('[Socket] Error:', err);
      setError(typeof err === 'string' ? err : (err.message || 'Connection error'));
    };
    
    const handleExtractionState = (data: ExtractionState) => {
      if (!isMounted.current) return;
      console.log('[Socket] Extraction state update received:', data);
      debugStateUpdate('extraction_state', data);
      
      // Make sure we have a complete state update
      if (data) {
        setState(prev => {
          const newState = {
            ...prev,
            ...data
          };
          console.log('[Socket] Updated extraction state:', newState);
          return newState;
        });
      }
    };
    
    const handleExtractionProgress = (data: { current_file: string; file_progress: number }) => {
      if (!isMounted.current) return;
      console.log('[Socket] Extraction progress update received:', data);
      debugStateUpdate('extraction_progress', data);
      
      setState(prev => {
        const newState = {
          ...prev,
          current_file: data.current_file,
          file_progress: data.file_progress,
          // Ensure the status is in_progress when we get progress updates
          status: 'in_progress'
        };
        console.log('[Socket] Updated progress state:', newState);
        return newState;
      });
    };
    
    const handleFileCompleted = (data: { 
      completed_file: string;
      processed_files: number;
      total_files: number;
      next_file: string | null;
    }) => {
      if (!isMounted.current) return;
      console.log('[Socket] File completed update received:', data);
      debugStateUpdate('file_completed', data);
      
      setState(prev => {
        const newState = {
          ...prev,
          processed_files: data.processed_files,
          total_files: data.total_files,
          current_file: data.next_file || prev.current_file,
          file_progress: data.next_file ? 0 : 1,
          // Ensure the status is in_progress when files are being processed
          status: 'in_progress'
        };
        console.log('[Socket] Updated file completed state:', newState);
        return newState;
      });
    };
    
    const handleMergedData = (data: { merged_data: any }) => {
      if (!isMounted.current || !data || !data.merged_data) return;
      console.log('[Socket] Merged data received, size:', Object.keys(data.merged_data).length);
      debugStateUpdate('merged_data', { size: Object.keys(data.merged_data).length });
      
      setState(prev => {
        const newState = {
          ...prev,
          merged_data: data.merged_data
        };
        console.log('[Socket] Updated merged data state');
        return newState;
      });
    };
    
    const handleSimplifiedMergedData = (data: SimplifiedMergedData) => {
      if (!isMounted.current) return;
      console.log('[Socket] Simplified merged data received:', data);
      debugStateUpdate('merged_data_simplified', data);
      
      setSimplifiedData(data);
      console.log('[Socket] Updated simplified data state');
    };
    
    const handleExtractionCompleted = (data: {
      success: boolean;
      message: string;
      processed_files: number;
      total_files: number;
      duration: number;
    }) => {
      if (!isMounted.current) return;
      console.log('[Socket] Extraction completed update received:', data);
      debugStateUpdate('extraction_completed', data);
      
      setState(prev => {
        const newState = {
          ...prev,
          status: data.success ? 'completed' : 'failed',
          message: data.message,
          duration: data.duration,
          processed_files: data.processed_files,
          total_files: data.total_files
        };
        console.log('[Socket] Updated completion state:', newState);
        return newState;
      });
      
      if (onComplete) {
        onComplete();
      }
    };
    
    const handleRoomJoined = (data: { room: string }) => {
      if (!isMounted.current) return;
      console.log('[Socket] Room joined event received:', data);
      debugStateUpdate('room_joined', data);
      
      if (data.room === roomId) {
        console.log(`[Socket] Joined room: ${data.room}`);
        setConnected(true);
        
        // Request current state after joining room
        console.log('[Socket] Requesting current extraction state');
        socket.current.emit('get_extraction_state', {
          source,
          dataset_name: datasetName
        });
      }
    };

    const handleFileChunksUpdated = (data: { 
      current_file: string;
      total_chunks: number;
    }) => {
      if (!isMounted.current) return;
      console.log('[Socket] File chunks updated received:', data);
      debugStateUpdate('file_chunks_updated', data);
      
      setState(prev => {
        const newState = {
          ...prev,
          current_file: data.current_file,
          current_file_chunks: data.total_chunks,
          current_file_chunk: 0,
          file_progress: 0
        };
        console.log('[Socket] Updated file chunks state:', newState);
        return newState;
      });
    };
    
    const handleChunkProgress = (data: {
      current_chunk: number;
      current_file_total_chunks: number;
      file_progress: number;
      total_processed_chunks: number;
      overall_total_chunks: number;
    }) => {
      if (!isMounted.current) return;
      console.log('[Socket] Chunk progress update received:', data);
      debugStateUpdate('chunk_progress', data);
      
      setState(prev => {
        const newState = {
          ...prev,
          current_file_chunk: data.current_chunk,
          current_file_chunks: data.current_file_total_chunks,
          file_progress: data.file_progress,
          processed_chunks: data.total_processed_chunks,
          total_chunks: data.overall_total_chunks
        };
        console.log('[Socket] Updated chunk progress state:', newState);
        return newState;
      });
    };

    // Update connection status if socket is already connected
    if (socket.current.connected) {
      console.log(`[Socket] Already connected, joining room ${roomId}`);
      setConnected(true);
      joinRoom(source, datasetName);
      
      // Also fetch current state
      console.log('[Socket] Fetching current state on initial setup');
      fetchCurrentState();
    } else {
      console.log('[Socket] Not connected yet, waiting for connect event');
    }
    
    // Set up event listeners
    console.log('[Socket] Setting up socket event listeners');
    socket.current.on('connect', handleConnect);
    socket.current.on('disconnect', handleDisconnect);
    socket.current.on('connect_error', handleError);
    socket.current.on('error', handleError);
    socket.current.on('extraction_state', handleExtractionState);
    socket.current.on('extraction_progress', handleExtractionProgress);
    socket.current.on('file_completed', handleFileCompleted);
    socket.current.on('merged_data', handleMergedData);
    socket.current.on('merged_data_simplified', handleSimplifiedMergedData);
    socket.current.on('extraction_completed', handleExtractionCompleted);
    socket.current.on('room_joined', handleRoomJoined);
    socket.current.on('file_chunks_updated', handleFileChunksUpdated);
    socket.current.on('chunk_progress', handleChunkProgress);

    // Set up auto-reconnect if we don't receive connection confirmation
    console.log('[Socket] Setting up auto-reconnect timer');
    reconnectTimerRef.current = setTimeout(() => {
      if (!connected && activeSession) {
        console.log('[Socket] Auto-reconnecting after timeout...');
        handleManualReconnect();
      }
    }, 3000);
    
    // Log all registered event listeners to confirm they're set up
    console.log('[Socket] Registered event listeners:', Object.keys(socket.current._callbacks || {}).join(', '));
    
    // Cleanup function
    return () => {
      console.log('[Socket] Cleaning up socket connection');
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      
      if (source && datasetName) {
        console.log(`[Socket] Leaving room ${roomId} on cleanup`);
        leaveRoom(source, datasetName);
      }
      
      // Remove all event listeners
      if (socket.current) {
        console.log('[Socket] Removing event listeners');
        socket.current.off('connect', handleConnect);
        socket.current.off('disconnect', handleDisconnect);
        socket.current.off('connect_error', handleError);
        socket.current.off('error', handleError);
        socket.current.off('extraction_state', handleExtractionState);
        socket.current.off('extraction_progress', handleExtractionProgress);
        socket.current.off('file_completed', handleFileCompleted);
        socket.current.off('merged_data', handleMergedData);
        socket.current.off('merged_data_simplified', handleSimplifiedMergedData);
        socket.current.off('extraction_completed', handleExtractionCompleted);
        socket.current.off('room_joined', handleRoomJoined);
        socket.current.off('file_chunks_updated', handleFileChunksUpdated);
        socket.current.off('chunk_progress', handleChunkProgress);
      }
    };
  }, [source, datasetName, roomId, activeSession, onComplete]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      isMounted.current = false;
      
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };
  }, []);

  // Direct state check from the server
  const fetchCurrentState = useCallback(async () => {
    if (!source || !datasetName) return;
    
    console.log(`[API] Fetching current extraction state for ${roomId}`);
    
    try {
      const response = await fetch(`http://localhost:5000/api/extraction/state?source=${encodeURIComponent(source)}&dataset_name=${encodeURIComponent(datasetName)}`);
      const data = await response.json();
      
      if (data.state) {
        console.log('[API] Received extraction state:', data.state);
        setState(data.state);
        
        // If we got valid state data, we must be connected
        setConnected(true);
        setReceivedData(true);
        
        // Debug: Also emit a get_state message via socket to compare responses
        if (socket.current && socket.current.connected) {
          console.log('[Socket] Manually requesting state via socket to verify connection');
          socket.current.emit('get_extraction_state', {
            source,
            dataset_name: datasetName
          });
        }
      } else {
        console.log('[API] No extraction state data received');
      }
    } catch (error) {
      console.error('[API] Error fetching extraction state:', error);
    }
  }, [source, datasetName, roomId]);

  const handleStartCheckingStatus = () => {
    checkExtractionSession();
  };

  const handleManualReconnect = () => {
    if (!source || !datasetName) return;
    
    console.log('[Socket] Manual reconnect requested');
    
    // Try to reconnect using the existing socket
    if (socket.current) {
      if (!socket.current.connected) {
        console.log('[Socket] Socket disconnected, attempting to reconnect');
        socket.current.connect();
        
        // After connect attempt, log connection status
        setTimeout(() => {
          console.log('[Socket] Connection status after reconnect attempt:', 
            socket.current?.connected ? 'Connected' : 'Disconnected');
        }, 500);
      } else {
        console.log('[Socket] Socket already connected');
      }
      
      // Rejoin the room
      console.log(`[Socket] Rejoining room ${roomId}`);
      joinRoom(source, datasetName);
      
      // Verify current listeners
      console.log('[Socket] Current event listeners:', 
        Object.keys(socket.current._callbacks || {}).join(', '));
      
      // Check if we're receiving events (don't try to re-add handlers as they're in a closure)
      if (!socket.current._callbacks || 
          !socket.current._callbacks['$extraction_state'] || 
          socket.current._callbacks['$extraction_state'].length === 0) {
        console.log('[Socket] Missing some event listeners - recommend refreshing the page');
        setError('Socket connection may be incomplete. Try refreshing the page if updates stop.');
      }
    }
    
    // Also fetch the current state directly
    console.log('[Socket] Fetching current state via API');
    fetchCurrentState();
    
    // If we've received data already, update connection status
    if (receivedData) {
      console.log('[Socket] Previously received data, updating connection status');
      setConnected(true);
    }
    
    setError('');
  };

  const handleToggleDetails = () => {
    setShowDetails(!showDetails);
  };

  const formattedMergedData = state.merged_data && Object.keys(state.merged_data).length > 0
    ? JSON.stringify(state.merged_data, null, 2)
    : simplifiedData 
      ? `Data available (${(simplifiedData.dataSize / 1024).toFixed(2)} KB) with keys: ${simplifiedData.keys.join(', ')}`
      : "No merged data available yet";
  
  const overallProgress = state.total_chunks > 0
    ? ((state.processed_chunks) / state.total_chunks * 100)
    : 0;

  // Show status check button if not active session
  if (!activeSession && !checkingStatus) {
    return (
      <Paper elevation={3} sx={{ p: 3, mt: 3, mb: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', flexDirection: 'column', py: 2 }}>
          <Typography variant="body1" sx={{ mb: 2 }}>
            There might be an ongoing extraction for this dataset.
          </Typography>
          <Button 
            variant="contained" 
            color="primary" 
            startIcon={<VisibilityIcon />}
            onClick={handleStartCheckingStatus}
          >
            Check Status of Current Extraction
          </Button>
          {error && (
            <Alert severity="error" sx={{ mt: 2, width: '100%' }}>
              {error}
            </Alert>
          )}
        </Box>
      </Paper>
    );
  }
  
  // Show loading indicator while checking
  if (checkingStatus) {
    return (
      <Paper elevation={3} sx={{ p: 3, mt: 3, mb: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', flexDirection: 'column', py: 2 }}>
          <CircularProgress size={36} sx={{ mb: 2 }} />
          <Typography>Checking extraction status...</Typography>
        </Box>
      </Paper>
    );
  }
  
  // Connection and data state indicator
  const connectionStatus = receivedData 
    ? { label: "Data Received", color: "success" as const }
    : connected 
      ? { label: "Connected", color: "success" as const }
      : { label: "Disconnected", color: "error" as const, icon: <WifiOffIcon /> };

  // Main progress view
  return (
    <Paper elevation={3} sx={{ p: 3, mt: 3, mb: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
        <Typography variant="h6">
          Extraction Progress
          <Chip 
            label={connectionStatus.label}
            color={connectionStatus.color}
            size="small" 
            sx={{ ml: 2 }} 
            icon={connectionStatus.icon}
          />
        </Typography>
        <Box>
          {!connected && (
            <Button
              variant="outlined"
              color="primary"
              size="small"
              startIcon={<RefreshIcon />}
              onClick={handleManualReconnect}
              sx={{ mr: 1 }}
            >
              Reconnect
            </Button>
          )}
          <IconButton onClick={handleToggleDetails}>
            {showDetails ? <ExpandLess /> : <ExpandMore />}
          </IconButton>
        </Box>
      </Box>
      
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      
      {state.status === 'interrupted' && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          {state.message || 'The extraction was interrupted due to a server restart.'}
          <Button 
            size="small" 
            color="warning" 
            sx={{ ml: 2 }} 
            onClick={async () => {
              try {
                const response = await fetch(
                  `http://localhost:5000/api/clear-extraction-state/${source}/${encodeURIComponent(datasetName)}`,
                  { method: 'POST' }
                );
                if (response.ok) {
                  checkExtractionSession();
                  setState({
                    status: 'idle',
                    total_files: 0,
                    processed_files: 0,
                    current_file: '',
                    file_progress: 0,
                    merged_data: {},
                    files: [],
                    total_chunks: 0,
                    processed_chunks: 0,
                    current_file_chunks: 0,
                    current_file_chunk: 0
                  });
                } else {
                  setError('Failed to clear extraction state');
                }
              } catch (err) {
                console.error('Error clearing state:', err);
                setError('Failed to clear extraction state');
              }
            }}
          >
            Clear State
          </Button>
        </Alert>
      )}
      
      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, mb: 2 }}>
        <Box sx={{ flex: '1 1 45%', minWidth: '200px' }}>
          <Typography variant="subtitle2">Status:</Typography>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            {state.status === 'in_progress' && (
              <CircularProgress size={20} sx={{ mr: 1 }} />
            )}
            <Typography>
              {state.status === 'idle' && 'Waiting to start'}
              {state.status === 'in_progress' && 'Extracting data...'}
              {state.status === 'completed' && 'Extraction completed'}
              {state.status === 'failed' && 'Extraction failed'}
              {state.status === 'interrupted' && 'Extraction interrupted'}
            </Typography>
          </Box>
        </Box>
        
        <Box sx={{ flex: '1 1 45%', minWidth: '200px' }}>
          <Typography variant="subtitle2">Files:</Typography>
          <Typography>
            {state.processed_files} / {state.total_files}
          </Typography>
        </Box>
      </Box>
      
      <Box sx={{ mb: 2 }}>
        <Typography variant="subtitle2">Current file:</Typography>
        <Typography noWrap>
          {state.current_file || 'None'} 
          {state.current_file_chunks > 0 && ` (Chunk ${state.current_file_chunk}/${state.current_file_chunks})`}
        </Typography>
      </Box>
      
      <Box sx={{ mb: 2 }}>
        <Typography variant="subtitle2" gutterBottom>
          File progress:
        </Typography>
        <LinearProgress 
          variant="determinate" 
          value={state.file_progress * 100} 
          sx={{ height: 10, borderRadius: 5 }}
        />
        <Typography variant="caption" color="textSecondary">
          {state.current_file_chunk} of {state.current_file_chunks} chunks processed
        </Typography>
      </Box>
      
      <Box sx={{ mb: 2 }}>
        <Typography variant="subtitle2" gutterBottom>
          Overall progress:
        </Typography>
        <LinearProgress 
          variant="determinate" 
          value={overallProgress} 
          sx={{ height: 10, borderRadius: 5 }}
        />
        <Typography variant="caption" color="textSecondary">
          {state.processed_chunks} of {state.total_chunks} total chunks processed ({state.processed_files} of {state.total_files} files)
        </Typography>
      </Box>
      
      <Collapse in={showDetails}>
        <Divider sx={{ my: 2 }} />
        
        <Typography variant="subtitle2" gutterBottom>
          Merged Data Preview:
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
        
        {state.message && (
          <Box sx={{ mt: 2 }}>
            <Typography variant="subtitle2">Message:</Typography>
            <Typography>{state.message}</Typography>
          </Box>
        )}
        
        {state.duration !== undefined && (
          <Box sx={{ mt: 2 }}>
            <Typography variant="subtitle2">Duration:</Typography>
            <Typography>{state.duration.toFixed(2)}s</Typography>
          </Box>
        )}
      </Collapse>
    </Paper>
  );
};

export default ExtractionProgress; 