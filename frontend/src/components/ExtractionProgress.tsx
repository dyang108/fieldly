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
  Sync as SyncIcon,
  Visibility as VisibilityIcon
} from '@mui/icons-material';
import { 
  getSocket, 
  joinRoom, 
  leaveRoom, 
  isSocketConnected, 
  forceNewConnection,
  isRoomActive
} from '../utils/socket';

// Constants for storage
interface ExtractionProgressProps {
  source: string;
  datasetName: string;
  onComplete?: () => void;
  initialMode?: 'active' | 'check'; // New prop to control initial mode
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
}

interface SimplifiedMergedData {
  dataSize: number;
  keys: string[];
  status: string;
}

const ExtractionProgress: React.FC<ExtractionProgressProps> = ({ source, datasetName, onComplete, initialMode = 'check' }) => {
  const [state, setState] = useState<ExtractionState>({
    status: 'idle',
    total_files: 0,
    processed_files: 0,
    current_file: '',
    file_progress: 0,
    merged_data: {},
    files: []
  });
  const [error, setError] = useState<string>('');
  const [connected, setConnected] = useState<boolean>(false);
  const [connecting, setConnecting] = useState<boolean>(false);
  const [simplifiedData, setSimplifiedData] = useState<SimplifiedMergedData | null>(null);
  const [showDetails, setShowDetails] = useState<boolean>(false);
  const [reconnectAttempts, setReconnectAttempts] = useState<number>(0);
  const [socketInitialized, setSocketInitialized] = useState<boolean>(false);
  const [checkingStatus, setCheckingStatus] = useState<boolean>(false);
  const [activeSession, setActiveSession] = useState<boolean>(initialMode === 'active');
  
  const isMounted = useRef(true);
  
  const roomId = `${source}_${datasetName}`;

  const checkExtractionSession = useCallback(async () => {
    if (!source || !datasetName) return;
    
    setCheckingStatus(true);
    setError('');
    
    try {
      // Call API to check if there's an active extraction session
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

  const connectToSocket = useCallback(() => {
    if (!source || !datasetName || !activeSession) return;
    
    console.log(`Setting up extraction progress connection for ${roomId}`);
    
    // Set connecting state right away
    setConnecting(true);
    
    const socket = getSocket();
    
    if (!socket) {
      console.log(`Socket not available for ${roomId}, cannot proceed`);
      setError('Could not connect to extraction service. Please try again.');
      setConnecting(false);
      return;
    }
    
    // Only set socketInitialized if we have a valid socket object
    setSocketInitialized(true);
    
    // If already connected, update state immediately
    if (socket.connected) {
      console.log('Socket already connected, joining room immediately');
      joinRoom(source, datasetName);
      setConnected(true);
      setConnecting(false);
    }
    
    const onConnect = () => {
      if (!isMounted.current) return;
      
      console.log('Socket connected in ExtractionProgress');
      setConnected(true);
      setConnecting(false);
      setError('');
      
      joinRoom(source, datasetName);
    };
    
    const onConnectError = (err: Error) => {
      if (!isMounted.current) return;
      
      console.error('Socket connection error:', err);
      setError(`Connection error: ${err.message}`);
      setConnected(false);
      setConnecting(false);
      
      setReconnectAttempts((prev) => prev + 1);
    };
    
    const onDisconnect = (reason: string) => {
      if (!isMounted.current) return;
      
      console.log(`Socket disconnected in ExtractionProgress: ${reason}`);
      setConnected(false);
      setConnecting(false);
      
      if (reason === 'io server disconnect' || reason === 'transport close') {
        setError(`Disconnected: ${reason}. Attempting to reconnect...`);
      }
    };
    
    const onExtractionState = (data: ExtractionState) => {
      if (!isMounted.current) return;
      
      console.log('Received extraction state:', data);
      setState(data);
    };
    
    const onExtractionProgress = (data: { current_file: string; file_progress: number }) => {
      if (!isMounted.current) return;
      
      console.log('Received progress update:', data);
      setState(prevState => ({
        ...prevState,
        current_file: data.current_file,
        file_progress: data.file_progress
      }));
    };
    
    const onMergedData = (data: { merged_data: any }) => {
      if (!isMounted.current) return;
      
      console.log('Received merged data update of size:', JSON.stringify(data).length);
      
      if (data && data.merged_data) {
        setState(prevState => ({
          ...prevState,
          merged_data: data.merged_data
        }));
        setSimplifiedData(null);
      } else {
        console.warn('Received invalid merged data:', data);
      }
    };
    
    const onSimplifiedMergedData = (data: SimplifiedMergedData) => {
      if (!isMounted.current) return;
      
      console.log('Received simplified merged data notification:', data);
      setSimplifiedData(data);
    };
    
    const onFileCompleted = (data: { 
      completed_file: string;
      processed_files: number;
      total_files: number;
      next_file: string | null;
    }) => {
      if (!isMounted.current) return;
      
      console.log('File completed:', data);
      setState(prevState => ({
        ...prevState,
        processed_files: data.processed_files,
        current_file: data.next_file || prevState.current_file,
        file_progress: data.next_file ? 0 : 1
      }));
    };
    
    const onExtractionCompleted = (data: {
      success: boolean;
      message: string;
      processed_files: number;
      total_files: number;
      duration: number;
    }) => {
      if (!isMounted.current) return;
      
      console.log('Extraction completed:', data);
      setState(prevState => ({
        ...prevState,
        status: data.success ? 'completed' : 'failed',
        message: data.message,
        duration: data.duration
      }));
      
      if (onComplete) {
        onComplete();
      }
    };
    
    const onError = (data: { message: string }) => {
      if (!isMounted.current) return;
      
      console.error('Socket error:', data);
      setError(data.message);
    };
    
    const onRoomJoined = (data: { room: string }) => {
      if (!isMounted.current) return;
      
      console.log(`Joined room: ${data.room}`);
      
      if (data.room === roomId) {
        setConnected(true);
        setConnecting(false);
      }
    };
    
    socket.on('connect', onConnect);
    socket.on('connect_error', onConnectError);
    socket.on('disconnect', onDisconnect);
    socket.on('extraction_state', onExtractionState);
    socket.on('extraction_progress', onExtractionProgress);
    socket.on('merged_data', onMergedData);
    socket.on('merged_data_simplified', onSimplifiedMergedData);
    socket.on('file_completed', onFileCompleted);
    socket.on('extraction_completed', onExtractionCompleted);
    socket.on('error', onError);
    socket.on('room_joined', onRoomJoined);
    
    return () => {
      console.log(`Cleaning up socket event listeners for ${roomId}`);
      
      socket.off('connect', onConnect);
      socket.off('connect_error', onConnectError);
      socket.off('disconnect', onDisconnect);
      socket.off('extraction_state', onExtractionState);
      socket.off('extraction_progress', onExtractionProgress);
      socket.off('merged_data', onMergedData);
      socket.off('merged_data_simplified', onSimplifiedMergedData);
      socket.off('file_completed', onFileCompleted);
      socket.off('extraction_completed', onExtractionCompleted);
      socket.off('error', onError);
      socket.off('room_joined', onRoomJoined);
    };
  }, [source, datasetName, roomId, activeSession, onComplete]);

  useEffect(() => {
    if (activeSession) {
      const cleanup = connectToSocket();
      return () => {
        if (cleanup) cleanup();
      };
    }
  }, [activeSession, connectToSocket]);

  useEffect(() => {
    return () => {
      isMounted.current = false;
      console.log(`Component unmounting for ${roomId}`);
      
      if (source && datasetName && connected) {
        console.log(`Leaving room ${roomId} on unmount`);
        leaveRoom(source, datasetName);
      }
    };
  }, [source, datasetName, roomId, connected]);

  const handleManualReconnect = useCallback(() => {
    if (!isMounted.current || !activeSession) return;
    
    setError('');
    setReconnectAttempts(prev => prev + 1);
    setConnecting(true);
    console.log('Manually reconnecting socket with force new connection...');
    
    const newSocket = forceNewConnection();
    
    setTimeout(() => {
      if (!isMounted.current) return;
      
      if (isSocketConnected()) {
        console.log('Socket reconnected successfully');
        setConnected(true);
        setConnecting(false);
        joinRoom(source, datasetName);
      } else {
        console.log('Socket failed to reconnect');
        setError('Failed to reconnect. Please try again.');
        setConnecting(false);
      }
    }, 1000);
  }, [source, datasetName, activeSession]);

  const handleStartCheckingStatus = () => {
    checkExtractionSession();
  };

  const formattedMergedData = state.merged_data && Object.keys(state.merged_data).length > 0
    ? JSON.stringify(state.merged_data, null, 2)
    : simplifiedData 
      ? `Data available (${(simplifiedData.dataSize / 1024).toFixed(2)} KB) with keys: ${simplifiedData.keys.join(', ')}`
      : "No merged data available yet";
  
  const overallProgress = state.total_files > 0
    ? ((state.processed_files) / state.total_files * 100)
    : 0;
  
  const handleToggleDetails = () => {
    setShowDetails(!showDetails);
  };
  
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
  
  return (
    <Paper elevation={3} sx={{ p: 3, mt: 3, mb: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
        <Typography variant="h6">
          Extraction Progress
          {connecting && (
            <Chip 
              label="Connecting..." 
              color="warning" 
              size="small" 
              sx={{ ml: 2 }} 
              icon={<SyncIcon />}
            />
          )}
          {!connected && !connecting && socketInitialized && (
            <Chip 
              label="Disconnected" 
              color="error" 
              size="small" 
              sx={{ ml: 2 }} 
              icon={<WifiOffIcon />}
            />
          )}
          {connected && (
            <Chip 
              label="Connected" 
              color="success" 
              size="small" 
              sx={{ ml: 2 }} 
            />
          )}
        </Typography>
        <Box>
          {!connected && !connecting && socketInitialized && (
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
          {reconnectAttempts > 0 && (
            <Box component="span" ml={1}>
              (Reconnect attempts: {reconnectAttempts})
            </Box>
          )}
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