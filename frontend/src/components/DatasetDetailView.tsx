import React, { useState, useEffect, useRef } from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  CircularProgress,
  Alert,
  List,
  ListItem,
  ListItemText,
  Divider,
  Chip,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Grid,
  Card,
  CardContent,
  Snackbar,
  IconButton,
  Breadcrumbs,
  Tooltip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Collapse,
  Dialog,
  DialogContent,
  DialogTitle,
  DialogActions
} from '@mui/material';
import {
  ArrowBack as ArrowBackIcon,
  Refresh as RefreshIcon,
  InsertDriveFile as FileIcon,
  Link as LinkIcon,
  Delete as DeleteIcon,
  CheckCircle as CheckCircleIcon,
  Error as ErrorIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
  Visibility as VisibilityIcon,
  Code as CodeIcon,
  PictureAsPdf as PdfIcon
} from '@mui/icons-material';
import axios from 'axios';
import { useParams, useNavigate, Link } from 'react-router-dom';
import ExtractionProgress from './ExtractionProgress';
import { 
  isRoomActive, 
  clearAllRoomData
} from '../utils/socket';

interface Schema {
  id: number;
  name: string;
  schema: object;
  created_at: string;
}

interface FileListItem {
  name: string;
  path: string;
  size?: number;
  last_modified?: string;
}

interface FileProcessingResult {
  filename: string;
  status: 'success' | 'error';
  output_file?: string;
  message?: string;
  content?: any;
}

interface ExtractionResult {
  dataset: string;
  output_directory: string;
  processed_files: number;
  results: FileProcessingResult[];
}

// Configure axios to use the API endpoint
const api = axios.create({
  baseURL: 'http://localhost:5000'
});

export default function DatasetDetailView() {
  const { source, datasetName } = useParams<{ source: string; datasetName: string }>();
  const navigate = useNavigate();
  
  const [loading, setLoading] = useState(true);
  const [files, setFiles] = useState<FileListItem[]>([]);
  const [schemas, setSchemas] = useState<Schema[]>([]);
  const [selectedSchemaId, setSelectedSchemaId] = useState<number | null>(null);
  const [error, setError] = useState('');
  const [notification, setNotification] = useState('');
  const [extracting, setExtracting] = useState(false);
  const [extractionResult, setExtractionResult] = useState<ExtractionResult | null>(null);
  const [showProgress, setShowProgress] = useState<boolean>(false);
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});
  const [selectedPdf, setSelectedPdf] = useState<string | null>(null);
  const [fileContent, setFileContent] = useState<Record<string, any>>({});
  const [resultsFetchTimer, setResultsFetchTimer] = useState<number | null>(null);

  // Reference to track extraction status to avoid unnecessary fetches
  const extractionStatusRef = useRef({
    lastFetchTime: 0,
    filesProcessed: 0,
    isCompleted: false
  });

  useEffect(() => {
    if (!source || !datasetName) {
      setError('Invalid dataset information');
      return;
    }
    
    // Set loading state to true while we fetch data
    setLoading(true);
    
    // First fetch general data and extraction results in parallel
    Promise.all([
      fetchData(),
      fetchExtractionResults()
    ]).then(() => {
      // After we have data, check if there's an active extraction
      checkForActiveExtraction();
      setLoading(false);
    }).catch(err => {
      console.error('Error loading dataset details:', err);
      setLoading(false);
    });
    
    return () => {
      // Clear any timers on unmount
      if (resultsFetchTimer) {
        clearTimeout(resultsFetchTimer);
      }
    };
  }, [source, datasetName]);

  // Replace the socket event listener with a refresh listener
  useEffect(() => {
    if (!source || !datasetName || !extracting) return;

    // Set up an extraction progress listener to trigger result refreshes
    const handleExtractionProgress = () => {
      // Throttle refreshes to avoid too many API calls
      const now = Date.now();
      if (now - extractionStatusRef.current.lastFetchTime > 5000) { // Refresh at most every 5 seconds
        console.log('Extraction progress detected, refreshing results...');
        fetchExtractionResults();
        extractionStatusRef.current.lastFetchTime = now;
      }
    };

    // Listen for file completions and general progress updates
    document.addEventListener('file_completed', handleExtractionProgress);
    document.addEventListener('extraction_progress', handleExtractionProgress);

    // Cleanup
    return () => {
      document.removeEventListener('file_completed', handleExtractionProgress);
      document.removeEventListener('extraction_progress', handleExtractionProgress);
    };
  }, [source, datasetName, extracting]);

  // Check if there's an active extraction session
  const checkForActiveExtraction = async () => {
    if (!source || !datasetName) return;
    
    try {
      const response = await api.get(
        `/api/extraction/status?source=${encodeURIComponent(source)}&dataset_name=${encodeURIComponent(datasetName)}`
      );
      
      if (response.data && response.data.is_active) {
        console.log('Active extraction session detected');
        setShowProgress(true);
      }
    } catch (err) {
      console.error('Error checking extraction status:', err);
    }
  };

  const fetchData = async () => {
    if (!source || !datasetName) return;
    
    try {
      // Fetch dataset files, schemas, and mapping information in parallel
      const [filesResponse, schemasResponse, mappingResponse] = await Promise.all([
        api.get(`/api/dataset-files/${source}/${encodeURIComponent(datasetName)}`),
        api.get('/api/schemas'),
        api.get(`/api/dataset-mapping/${source}/${encodeURIComponent(datasetName)}`)
      ]);

      // Handle file data - could be array of strings or objects
      const filesData = filesResponse.data.files;
      if (Array.isArray(filesData)) {
        setFiles(filesData.map((file) => {
          if (typeof file === 'string') {
            return {
              name: file,
              path: `${source}/${datasetName}/${file}`,
              size: 0
            };
          } else {
            return file;
          }
        }));
      } else {
        // Handle unexpected data format
        console.error('Unexpected file data format:', filesData);
        setFiles([]);
        setError('Failed to parse file data');
      }
      
      setSchemas(schemasResponse.data);
      
      // Set the selected schema if a mapping exists
      if (mappingResponse.data && mappingResponse.data.schema_id) {
        setSelectedSchemaId(mappingResponse.data.schema_id);
      }
      
      setError('');
    } catch (err) {
      console.error('Error fetching data:', err);
      setError('Failed to fetch dataset information');
    }
  };

  // Function to fetch extraction results
  const fetchExtractionResults = async () => {
    if (!source || !datasetName) return;
    
    try {
      console.log(`Fetching extraction results for ${source}/${datasetName}`);
      
      const response = await api.get(
        `/api/extraction-results/${source}/${encodeURIComponent(datasetName)}`
      );
      
      // If we get valid results, update the state
      if (response.data && Array.isArray(response.data.results)) {
        const resultsCount = response.data.results.length;
        console.log(`Got ${resultsCount} extraction results`);
        
        if (resultsCount > 0) {
          console.log('Sample result:', response.data.results[0]);
        }
        
        setExtractionResult({
          dataset: datasetName,
          output_directory: response.data.output_directory,
          processed_files: response.data.processed_files,
          results: response.data.results
        });
        
        // Update our tracking to avoid unnecessary refreshes
        extractionStatusRef.current.filesProcessed = response.data.processed_files;
      } else {
        console.log('No extraction results found or invalid response format');
      }
    } catch (err) {
      console.error('Error fetching extraction results:', err);
      // Don't show error to user as this might just be polling
    }
  };

  const handleSchemaChange = async (schemaId: number | null) => {
    try {
      setSelectedSchemaId(schemaId);
      
      await api.post('/api/dataset-mappings', {
        dataset_name: datasetName,
        source: source,
        schema_id: schemaId
      });
      
      setNotification('Schema mapping updated successfully');
      setTimeout(() => setNotification(''), 3000);
    } catch (err) {
      console.error('Error updating mapping:', err);
      setError('Failed to update schema mapping');
      setTimeout(() => setError(''), 5000);
    }
  };

  const handleExtractData = async () => {
    if (!selectedSchemaId) {
      setError('Please select a schema first');
      return;
    }

    setExtracting(true);
    setExtractionResult(null);
    setShowProgress(true);
    setError('');
    setExpandedRows({});
    
    // Reset extraction tracking
    extractionStatusRef.current = {
      lastFetchTime: 0,
      filesProcessed: 0,
      isCompleted: false
    };
    
    try {
      // Get the schema object
      const schema = schemas.find(s => s.id === selectedSchemaId);
      if (!schema) {
        throw new Error('Selected schema not found');
      }
      
      // Start extraction process
      const response = await api.post(
        `/api/extract/${source!}/${encodeURIComponent(datasetName!)}`,
        schema.schema,
        {
          headers: {
            'Content-Type': 'application/json'
          }
        }
      );
      
      setNotification(`Data extraction completed for ${datasetName}`);
      
      // Fetch the results after extraction completes
      fetchExtractionResults();
      
      // Mark as completed
      extractionStatusRef.current.isCompleted = true;
    } catch (err) {
      console.error('Error extracting data:', err);
      setError('Failed to extract data from dataset');
    } finally {
      setExtracting(false);
    }
  };

  const handleExtractionComplete = () => {
    console.log('Extraction completed, hiding progress component');
    setShowProgress(false);
    
    // Fetch the final results when extraction completes
    fetchExtractionResults();
    
    // Mark as completed
    extractionStatusRef.current.isCompleted = true;
  };

  const toggleExpandRow = (filename: string) => {
    setExpandedRows(prev => ({
      ...prev,
      [filename]: !prev[filename]
    }));

    // Fetch file content if not already loaded
    if (!fileContent[filename] && extractionResult) {
      const result = extractionResult.results.find(r => r.filename === filename);
      if (result && result.status === 'success' && result.output_file) {
        fetchFileContent(result.output_file);
      }
    }
  };

  const fetchFileContent = async (outputFile: string) => {
    try {
      const response = await api.get(`/api/file-content?path=${encodeURIComponent(outputFile)}`);
      if (response.data && response.data.content) {
        setFileContent(prev => ({
          ...prev,
          [outputFile]: response.data.content
        }));
      }
    } catch (err) {
      console.error('Error fetching file content:', err);
      setError(`Failed to fetch content for ${outputFile}`);
      setTimeout(() => setError(''), 3000);
    }
  };

  const handlePreviewPdf = (filename: string) => {
    if (!source || !datasetName) {
      setError('Source or dataset name is undefined');
      return;
    }
    setSelectedPdf(`http://localhost:5000/api/preview-file/${source}/${encodeURIComponent(datasetName)}/${encodeURIComponent(filename)}`);
  };

  const handleClosePdfPreview = () => {
    setSelectedPdf(null);
  };
  
  // Function to reset all socket room data
  const handleClearAllRooms = () => {
    console.log('DatasetDetailView: Clearing all room data');
    clearAllRoomData();
    setShowProgress(false);
    setNotification('All room data cleared');
    setTimeout(() => setNotification(''), 3000);
  };

  // Determine which results to show - now we only use extractionResult
  const resultsToShow = extractionResult ? extractionResult.results : [];

  if (loading) {
    return (
      <Box sx={{ p: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'center', my: 2 }}>
          <CircularProgress />
        </Box>
      </Box>
    );
  }

  return (
    <Box sx={{ p: 3 }}>
      {/* Breadcrumb navigation */}
      <Breadcrumbs sx={{ mb: 3 }}>
        <Link to="/" style={{ textDecoration: 'none', display: 'flex', alignItems: 'center' }}>
          <IconButton size="small">
            <ArrowBackIcon fontSize="small" />
          </IconButton>
          <Typography color="text.primary">Datasets</Typography>
        </Link>
        <Typography color="text.primary">{datasetName}</Typography>
      </Breadcrumbs>
      
      {/* Header with dataset info */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h4" component="h1" gutterBottom>
            {datasetName}
          </Typography>
          <Chip 
            label={source?.toUpperCase()} 
            color={source === 'local' ? 'info' : 'warning'} 
            size="small" 
          />
        </Box>
        
        <Box>
          <Tooltip title="Reset socket rooms">
            <IconButton
              color="error"
              onClick={handleClearAllRooms}
              sx={{ mr: 1 }}
            >
              <DeleteIcon />
            </IconButton>
          </Tooltip>
          <Button
            startIcon={<RefreshIcon />}
            variant="outlined"
            onClick={fetchData}
          >
            Refresh
          </Button>
        </Box>
      </Box>
      
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      
      <Snackbar
        open={!!notification}
        message={notification}
        anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
        onClose={() => setNotification('')}
      />
      
      {/* Show progress component conditionally */}
      {showProgress && source && datasetName ? (
        <ExtractionProgress 
          source={source} 
          datasetName={datasetName} 
          initialMode={extracting ? 'active' : 'check'} 
          onComplete={handleExtractionComplete}
        />
      ) : null}
      
      {/* Main content area */}
      <Box sx={{ display: 'flex', flexDirection: { xs: 'column', md: 'row' }, gap: 3 }}>
        {/* Left side content */}
        <Box sx={{ flex: { xs: '1 1 100%', md: '7 1 0%' }, display: 'flex', flexDirection: 'column', gap: 3 }}>
          {/* File list */}
          <Paper elevation={2} sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Files ({files.length})
            </Typography>
            
            {files.length === 0 ? (
              <Typography color="text.secondary" align="center" sx={{ py: 2 }}>
                No files found in this dataset
              </Typography>
            ) : (
              <List sx={{ maxHeight: 300, overflow: 'auto' }}>
                {files.map((file, index) => (
                  <React.Fragment key={file.path}>
                    <ListItem
                      secondaryAction={
                        <IconButton 
                          edge="end" 
                          onClick={() => handlePreviewPdf(file.name)}
                          disabled={!file.name.toLowerCase().endsWith('.pdf')}
                        >
                          <PdfIcon />
                        </IconButton>
                      }
                    >
                      <FileIcon sx={{ mr: 1, color: 'text.secondary' }} />
                      <ListItemText 
                        primary={file.name} 
                        secondary={file.size ? `${(file.size / 1024 / 1024).toFixed(2)} MB` : null}
                      />
                    </ListItem>
                    {index < files.length - 1 && <Divider />}
                  </React.Fragment>
                ))}
              </List>
            )}
          </Paper>
          
          {/* Extraction Results table */}
          <Paper elevation={2} sx={{ p: 2 }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
              <Typography variant="h6">
                Extraction Results
                {extractionResult && (
                  <Typography variant="body2" component="span" sx={{ ml: 2 }}>
                    ({extractionResult.processed_files} files processed)
                  </Typography>
                )}
              </Typography>
              <Button
                startIcon={<RefreshIcon />}
                size="small"
                onClick={fetchExtractionResults}
                variant="outlined"
              >
                Refresh Results
              </Button>
            </Box>
            
            {loading ? (
              <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', py: 4 }}>
                <CircularProgress size={40} sx={{ mb: 2 }} />
                <Typography variant="body1">Loading extraction results...</Typography>
              </Box>
            ) : resultsToShow.length === 0 ? (
              <Box sx={{ textAlign: 'center', py: 4, backgroundColor: 'background.paper' }}>
                <Typography variant="body1" color="text.secondary">
                  No extraction results found for this dataset.
                </Typography>
                <Button 
                  variant="text" 
                  startIcon={<RefreshIcon />} 
                  onClick={fetchExtractionResults}
                  sx={{ mt: 2 }}
                >
                  Refresh
                </Button>
              </Box>
            ) : (
              <TableContainer sx={{ maxHeight: 400, overflow: 'auto' }}>
                <Table stickyHeader size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell width="5%"></TableCell>
                      <TableCell width="10%">Status</TableCell>
                      <TableCell width="30%">Filename</TableCell>
                      <TableCell width="45%">Details</TableCell>
                      <TableCell width="10%">Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {resultsToShow.map((result, index) => (
                      <React.Fragment key={index}>
                        <TableRow 
                          sx={{ 
                            '&:nth-of-type(odd)': { backgroundColor: 'action.hover' },
                            backgroundColor: result.status === 'success' ? 'rgba(76, 175, 80, 0.08)' : 'rgba(239, 83, 80, 0.08)'
                          }}
                        >
                          <TableCell>
                            <IconButton 
                              size="small" 
                              onClick={() => toggleExpandRow(result.filename)}
                              disabled={result.status !== 'success'}
                            >
                              {expandedRows[result.filename] ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                            </IconButton>
                          </TableCell>
                          <TableCell>
                            {result.status === 'success' ? (
                              <CheckCircleIcon color="success" fontSize="small" />
                            ) : (
                              <ErrorIcon color="error" fontSize="small" />
                            )}
                          </TableCell>
                          <TableCell>{result.filename}</TableCell>
                          <TableCell>
                            {result.status === 'success'
                              ? result.output_file
                              : result.message
                            }
                          </TableCell>
                          <TableCell>
                            <Tooltip title="Preview Original PDF">
                              <IconButton 
                                size="small" 
                                onClick={() => handlePreviewPdf(result.filename)}
                                disabled={!result.filename.toLowerCase().endsWith('.pdf')}
                              >
                                <PdfIcon />
                              </IconButton>
                            </Tooltip>
                            {result.status === 'success' && (
                              <Tooltip title="View Extracted Data">
                                <IconButton 
                                  size="small" 
                                  onClick={() => toggleExpandRow(result.filename)}
                                >
                                  <CodeIcon />
                                </IconButton>
                              </Tooltip>
                            )}
                          </TableCell>
                        </TableRow>
                        {result.status === 'success' && result.output_file && (
                          <TableRow>
                            <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={5}>
                              <Collapse in={expandedRows[result.filename]} timeout="auto" unmountOnExit>
                                <Box sx={{ margin: 1, p: 1, backgroundColor: 'background.paper' }}>
                                  <Typography variant="h6" gutterBottom component="div">
                                    Extracted Content
                                  </Typography>
                                  <Box 
                                    sx={{ 
                                      p: 2, 
                                      overflowX: 'auto',
                                      fontFamily: 'monospace',
                                      fontSize: '0.875rem',
                                      backgroundColor: 'grey.100',
                                      borderRadius: 1
                                    }}
                                  >
                                    {result.output_file && fileContent[result.output_file] ? (
                                      <pre>{JSON.stringify(fileContent[result.output_file], null, 2)}</pre>
                                    ) : (
                                      <Box sx={{ display: 'flex', justifyContent: 'center', p: 2 }}>
                                        <CircularProgress size={24} />
                                      </Box>
                                    )}
                                  </Box>
                                </Box>
                              </Collapse>
                            </TableCell>
                          </TableRow>
                        )}
                      </React.Fragment>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </Paper>
        </Box>
        
        {/* Right side - Schema selection and actions */}
        <Box sx={{ flex: { xs: '1 1 100%', md: '5 1 0%' } }}>
          <Paper elevation={2} sx={{ p: 2, mb: 3 }}>
            <Typography variant="h6" gutterBottom>
              Schema Selection
            </Typography>
            
            <FormControl fullWidth sx={{ mb: 2 }}>
              <InputLabel id="schema-select-label">Select Schema</InputLabel>
              <Select
                labelId="schema-select-label"
                value={selectedSchemaId?.toString() || ''}
                label="Select Schema"
                onChange={(e) => handleSchemaChange(e.target.value ? Number(e.target.value) : null)}
              >
                <MenuItem value="">
                  <em>None</em>
                </MenuItem>
                {schemas.map((schema) => (
                  <MenuItem key={schema.id} value={schema.id.toString()}>
                    {schema.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            
            <Button
              variant="contained"
              color="primary"
              fullWidth
              startIcon={extracting ? <CircularProgress size={20} color="inherit" /> : <LinkIcon />}
              disabled={!selectedSchemaId || extracting}
              onClick={handleExtractData}
            >
              {extracting ? 'Extracting...' : 'Extract Data with Selected Schema'}
            </Button>
          </Paper>
        </Box>
      </Box>
      
      {/* PDF Preview Dialog */}
      <Dialog
        open={!!selectedPdf}
        onClose={handleClosePdfPreview}
        maxWidth="lg"
        fullWidth
      >
        <DialogTitle>
          PDF Preview
          <IconButton
            aria-label="close"
            onClick={handleClosePdfPreview}
            sx={{
              position: 'absolute',
              right: 8,
              top: 8,
            }}
          >
            <ArrowBackIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          {selectedPdf && (
            <iframe
              src={selectedPdf}
              width="100%"
              height="600px"
              title="PDF Preview"
              style={{ border: 'none' }}
            />
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClosePdfPreview}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
} 