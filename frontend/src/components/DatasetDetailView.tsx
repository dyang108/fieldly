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
  PictureAsPdf as PdfIcon,
  Timeline as TimelineIcon
} from '@mui/icons-material';
import axios from 'axios';
import { useParams, useNavigate, Link as RouterLink } from 'react-router-dom';

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
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});
  const [selectedPdf, setSelectedPdf] = useState<string | null>(null);
  const [fileContent, setFileContent] = useState<Record<string, any>>({});
  const [resultsFetchTimer, setResultsFetchTimer] = useState<number | null>(null);
  const [hasActiveExtraction, setHasActiveExtraction] = useState<boolean>(false);

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
      fetchExtractionResults(),
      checkForActiveExtraction()
    ]).then(() => {
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

  // Set up a polling mechanism to check for extraction updates
  useEffect(() => {
    if (!source || !datasetName || !extracting) return;

    // Set up an interval to check for updates
    const pollingInterval = setInterval(() => {
      // Throttle refreshes to avoid too many API calls
      const now = Date.now();
      if (now - extractionStatusRef.current.lastFetchTime > 5000) { // Refresh at most every 5 seconds
        console.log('Polling for extraction results...');
        fetchExtractionResults();
        checkForActiveExtraction();
        extractionStatusRef.current.lastFetchTime = now;
      }
    }, 10000); // Poll every 10 seconds

    // Cleanup
    return () => {
      clearInterval(pollingInterval);
    };
  }, [source, datasetName, extracting]);

  // Check if there's an active extraction session
  const checkForActiveExtraction = async () => {
    if (!source || !datasetName) return;
    
    try {
      const response = await api.get(
        `/api/extraction-progress/check/${encodeURIComponent(source)}/${encodeURIComponent(datasetName)}`
      );
      
      if (response.data && response.data.active) {
        console.log('Active extraction session detected');
        setHasActiveExtraction(true);
      } else {
        setHasActiveExtraction(false);
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
    if (!source || !datasetName) {
      setError('Source or dataset name is undefined');
      return;
    }
    
    if (!selectedSchemaId) {
      setError('Please select a schema first');
      return;
    }
    
    setError('');
    setExtracting(true);
    
    // Only clear extraction results if starting a new extraction, not when resuming
    if (!hasActiveExtraction) {
      setExtractionResult(null);
      setExpandedRows({});
    }
    
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
      
      let response;
      
      // If we have an active extraction, use the resume endpoint
      if (hasActiveExtraction) {
        console.log('Resuming existing extraction for:', datasetName);
        
        response = await api.post(
          `/api/extraction-progress/resume-extraction/${source!}/${encodeURIComponent(datasetName!)}`,
          {}, // Empty body since the schema is already stored
          {
            headers: {
              'Content-Type': 'application/json'
            }
          }
        );
        
        setNotification(`Resuming data extraction for ${datasetName}`);
      } else {
        console.log('Starting new extraction for:', datasetName);
        
        // Start extraction process
        response = await api.post(
          `/api/extract/${source!}/${encodeURIComponent(datasetName!)}`,
          schema.schema,
          {
            headers: {
              'Content-Type': 'application/json'
            }
          }
        );
        
        setNotification(`Data extraction started for ${datasetName}`);
      }
      
      // Navigate to the extraction progress page
      navigate(`/extraction-progress/${source}/${datasetName}`);
      
    } catch (err) {
      console.error('Error extracting data:', err);
      setError('Failed to extract data from dataset');
      setExtracting(false);
    }
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
  
  const handleDeleteRunningExtraction = async () => {
    if (!source || !datasetName) {
      setError('Source or dataset name is undefined');
      return;
    }
    
    try {
      setLoading(true);
      const response = await api.post(
        `/api/delete-running-extraction/${source}/${encodeURIComponent(datasetName)}`
      );
      
      if (response.data.success) {
        setNotification(response.data.message || 'Running extraction deleted successfully');
        setHasActiveExtraction(false);
      } else {
        setError(response.data.message || 'Failed to delete running extraction');
      }
    } catch (err) {
      console.error('Error deleting running extraction:', err);
      setError('Failed to delete running extraction');
    } finally {
      setLoading(false);
    }
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
      {/* Header with back button, title, and extract button */}
      <Box sx={{ display: 'flex', alignItems: 'center', mb: 3, flexWrap: 'wrap', gap: 2 }}>
        <Button
          variant="outlined"
          startIcon={<ArrowBackIcon />}
          onClick={() => navigate('/')}
          sx={{ mr: 2 }}
        >
          Back to Datasets
        </Button>
        
        <Breadcrumbs aria-label="breadcrumb" sx={{ flexGrow: 1 }}>
          <Typography color="text.primary">
            {source && source.charAt(0).toUpperCase() + source.slice(1)}
          </Typography>
          <Typography color="text.primary">{datasetName}</Typography>
        </Breadcrumbs>
        
        <Box sx={{ display: 'flex', gap: 2 }}>
          {/* Add extraction progress button when active */}
          {hasActiveExtraction && (
            <>
              <Button
                component={RouterLink}
                to={`/extraction-progress/${source}/${datasetName}`}
                variant="outlined"
                color="primary"
                startIcon={<TimelineIcon />}
              >
                View Extraction Progress
              </Button>
              
              <Button
                variant="outlined"
                color="error"
                startIcon={<DeleteIcon />}
                onClick={handleDeleteRunningExtraction}
                disabled={loading}
              >
                Delete Running Extraction
              </Button>
            </>
          )}
          
          <Button 
            variant="contained" 
            disabled={extracting || !selectedSchemaId}
            onClick={handleExtractData}
            startIcon={extracting ? <CircularProgress size={20} /> : undefined}
          >
            {hasActiveExtraction ? 'Resume Extraction' : 'Extract Data'}
          </Button>
          
          <Button
            variant="outlined"
            startIcon={<RefreshIcon />}
            onClick={() => {
              setLoading(true);
              Promise.all([fetchData(), fetchExtractionResults(), checkForActiveExtraction()])
                .then(() => setLoading(false))
                .catch(err => {
                  console.error('Error refreshing data:', err);
                  setLoading(false);
                });
            }}
          >
            Refresh
          </Button>
        </Box>
      </Box>
      
      {/* Show error message if any */}
      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}
      
      {/* Show notification */}
      <Snackbar
        open={!!notification}
        autoHideDuration={6000}
        onClose={() => setNotification('')}
        message={notification}
      />
      
      {/* Loading indicator */}
      {loading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
          <CircularProgress />
        </Box>
      )}

      {/* Main content area */}
      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 3 }}>
        {/* Left side - Files list */}
        <Box sx={{ flex: '1 1 48%', minWidth: '300px' }}>
          <Card>
            <CardContent>
              <Typography variant="h6" component="h2" gutterBottom>
                Dataset Files {files.length > 0 && `(${files.length})`}
              </Typography>
              
              {/* Show file list */}
              {files.length > 0 ? (
                <List sx={{ maxHeight: '300px', overflow: 'auto' }}>
                  {files.map((file, index) => (
                    <React.Fragment key={file.path}>
                      {index > 0 && <Divider />}
                      <ListItem
                        secondaryAction={
                          <IconButton 
                            edge="end" 
                            aria-label="view" 
                            onClick={() => handlePreviewPdf(file.name)}
                            disabled={!file.name.toLowerCase().endsWith('.pdf')}
                          >
                            <PdfIcon />
                          </IconButton>
                        }
                      >
                        <ListItemText 
                          primary={file.name} 
                          secondary={file.size ? `${(file.size / 1024).toFixed(2)} KB` : ''} 
                        />
                      </ListItem>
                    </React.Fragment>
                  ))}
                </List>
              ) : (
                <Box sx={{ py: 2, textAlign: 'center' }}>
                  <Typography color="text.secondary">
                    No files found in this dataset
                  </Typography>
                </Box>
              )}
            </CardContent>
          </Card>
        </Box>

        {/* Right side - Schema selection */}
        <Box sx={{ flex: '1 1 48%', minWidth: '300px' }}>
          <Card>
            <CardContent>
              <Typography variant="h6" component="h2" gutterBottom>
                Schema Selection
              </Typography>
              
              <FormControl fullWidth sx={{ mt: 2 }}>
                <InputLabel id="schema-select-label">Select Schema</InputLabel>
                <Select
                  labelId="schema-select-label"
                  value={selectedSchemaId || ''}
                  label="Select Schema"
                  onChange={(e) => handleSchemaChange(e.target.value ? Number(e.target.value) : null)}
                >
                  <MenuItem value="">
                    <em>None</em>
                  </MenuItem>
                  {schemas.map((schema) => (
                    <MenuItem key={schema.id} value={schema.id}>
                      {schema.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              
              {/* Schema view button */}
              <Box sx={{ mt: 2, display: 'flex', justifyContent: 'space-between' }}>
                <Button
                  component={RouterLink}
                  to="/schemas"
                  variant="outlined"
                  startIcon={<CodeIcon />}
                >
                  Manage Schemas
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Box>
      </Box>

      {/* Extraction Results - below the two cards */}
      <Box sx={{ mt: 3 }}>
        <Paper elevation={2} sx={{ p: 2 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
            <Typography variant="h6">Extraction Results</Typography>
            {extractionResult && (
              <Typography variant="body2" component="span" sx={{ ml: 2 }}>
                ({extractionResult.processed_files} files processed)
              </Typography>
            )}
            <Button
              startIcon={<RefreshIcon />}
              size="small"
              onClick={fetchExtractionResults}
              variant="outlined"
            >
              Refresh Results
            </Button>
          </Box>
          
          {/* Results content */}
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
                          backgroundColor: result.status === 'error' ? 'rgba(255, 0, 0, 0.05)' : undefined
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
                          {result.status === 'success' ? (
                            `Extracted data saved to ${result.output_file}`
                          ) : (
                            result.message || 'Error extracting data'
                          )}
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
                          {' '}
                          <Tooltip title="View Extracted Data">
                            <IconButton
                              size="small"
                              onClick={() => toggleExpandRow(result.filename)}
                            >
                              <CodeIcon />
                            </IconButton>
                          </Tooltip>
                        </TableCell>
                      </TableRow>
                      {/* Expanded row for file content */}
                      {expandedRows[result.filename] && (
                        <TableRow>
                          <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={5}>
                            <Collapse in={expandedRows[result.filename]} timeout="auto" unmountOnExit>
                              <Box sx={{ margin: 1, p: 1, backgroundColor: 'background.paper' }}>
                                <Typography variant="h6" gutterBottom component="div">
                                  Extracted Data
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