import React from 'react';
import { useState, useEffect } from 'react';
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
  const [partialResults, setPartialResults] = useState<FileProcessingResult[]>([]);
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});
  const [selectedPdf, setSelectedPdf] = useState<string | null>(null);
  const [fileContent, setFileContent] = useState<Record<string, any>>({});

  useEffect(() => {
    if (!source || !datasetName) {
      setError('Invalid dataset information');
      return;
    }
    
    fetchData();
  }, [source, datasetName]);

  // New socket event handler for file completions
  useEffect(() => {
    if (!source || !datasetName || !extracting) return;

    // Set up event listener for file completions
    const handleFileCompletion = (data: any) => {
      console.log('File processing completed:', data);
      if (data && data.result) {
        setPartialResults(prev => {
          // Check if we already have this file result
          const existingIndex = prev.findIndex(r => r.filename === data.result.filename);
          if (existingIndex >= 0) {
            // Replace existing entry
            const updatedResults = [...prev];
            updatedResults[existingIndex] = data.result;
            return updatedResults;
          } else {
            // Add new entry
            return [...prev, data.result];
          }
        });
      }
    };

    // Add event listener to socket
    document.addEventListener('file_processed', (e: any) => handleFileCompletion(e.detail));

    // Cleanup
    return () => {
      document.removeEventListener('file_processed', (e: any) => handleFileCompletion(e.detail));
    };
  }, [source, datasetName, extracting]);

  const fetchData = async () => {
    setLoading(true);
    try {
      // Fetch dataset files, schemas, and mapping information in parallel
      const [filesResponse, schemasResponse, mappingResponse] = await Promise.all([
        api.get(`/api/dataset-files/${source!}/${encodeURIComponent(datasetName!)}`),
        api.get('/api/schemas'),
        api.get(`/api/dataset-mapping/${source!}/${encodeURIComponent(datasetName!)}`)
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
    } finally {
      setLoading(false);
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
    setPartialResults([]);
    setExpandedRows({});
    
    try {
      // Get the schema object
      const schema = schemas.find(s => s.id === selectedSchemaId);
      if (!schema) {
        throw new Error('Selected schema not found');
      }
      
      const response = await api.post(
        `/api/extract/${source!}/${encodeURIComponent(datasetName!)}`,
        schema.schema,
        {
          headers: {
            'Content-Type': 'application/json'
          }
        }
      );
      
      setExtractionResult(response.data);
      setNotification(`Data extraction completed for ${datasetName}`);
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
    
    // Refresh data to ensure we have the latest results
    fetchData();
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

  // Determine which results to show
  const resultsToShow = extractionResult ? extractionResult.results : 
                       partialResults.length > 0 ? partialResults : [];

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
          {resultsToShow.length > 0 && (
            <Paper elevation={2} sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Extraction Results
                {extractionResult && (
                  <Typography variant="body2" component="span" sx={{ ml: 2 }}>
                    ({extractionResult.processed_files} files processed)
                  </Typography>
                )}
                {partialResults.length > 0 && !extractionResult && (
                  <Typography variant="body2" component="span" sx={{ ml: 2 }}>
                    ({partialResults.length} files processed so far)
                  </Typography>
                )}
              </Typography>
              
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
            </Paper>
          )}
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