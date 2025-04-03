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
  Breadcrumbs
} from '@mui/material';
import {
  ArrowBack as ArrowBackIcon,
  Refresh as RefreshIcon,
  InsertDriveFile as FileIcon,
  Link as LinkIcon
} from '@mui/icons-material';
import axios from 'axios';
import { useParams, useNavigate, Link } from 'react-router-dom';

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

interface ExtractionResult {
  dataset: string;
  output_directory: string;
  processed_files: number;
  results: Array<{
    filename: string;
    status: 'success' | 'error';
    output_file?: string;
    message?: string;
  }>;
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

  useEffect(() => {
    if (!source || !datasetName) {
      setError('Invalid dataset information');
      return;
    }
    
    fetchData();
  }, [source, datasetName]);

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
        
        <Button
          startIcon={<RefreshIcon />}
          variant="outlined"
          onClick={fetchData}
        >
          Refresh
        </Button>
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
      
      {/* Main content area */}
      <Box sx={{ display: 'flex', flexDirection: { xs: 'column', md: 'row' }, gap: 3 }}>
        {/* Left side - File list */}
        <Box sx={{ flex: { xs: '1 1 100%', md: '7 1 0%' } }}>
          <Paper elevation={2} sx={{ height: '100%', p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Files ({files.length})
            </Typography>
            
            {files.length === 0 ? (
              <Typography color="text.secondary" align="center" sx={{ py: 2 }}>
                No files found in this dataset
              </Typography>
            ) : (
              <List sx={{ maxHeight: 500, overflow: 'auto' }}>
                {files.map((file, index) => (
                  <React.Fragment key={file.path}>
                    <ListItem>
                      <FileIcon sx={{ mr: 1, color: 'text.secondary' }} />
                      <ListItemText 
                        primary={file.name} 
                        // Display file size if available
                        secondary={file.size ? `${(file.size / 1024 / 1024).toFixed(2)} MB` : null}
                      />
                    </ListItem>
                    {index < files.length - 1 && <Divider />}
                  </React.Fragment>
                ))}
              </List>
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
          
          {/* Extraction Results */}
          {extractionResult && (
            <Paper elevation={2} sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Extraction Results
              </Typography>
              
              <Typography variant="body2" gutterBottom>
                <strong>Processed Files:</strong> {extractionResult.processed_files}
              </Typography>
              
              <Typography variant="body2" gutterBottom>
                <strong>Output Directory:</strong> {extractionResult.output_directory}
              </Typography>
              
              <Typography variant="subtitle2" sx={{ mt: 2, mb: 1 }}>
                File Results:
              </Typography>
              
              <List sx={{ maxHeight: 200, overflow: 'auto' }}>
                {extractionResult.results.map((result, index) => (
                  <ListItem key={index}>
                    <ListItemText
                      primary={result.filename}
                      secondary={
                        result.status === 'success'
                          ? `Extracted to ${result.output_file}`
                          : result.message
                      }
                      primaryTypographyProps={{
                        color: result.status === 'success' ? 'success.main' : 'error.main'
                      }}
                    />
                  </ListItem>
                ))}
              </List>
            </Paper>
          )}
        </Box>
      </Box>
    </Box>
  );
} 