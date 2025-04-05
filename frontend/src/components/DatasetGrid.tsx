import React from 'react';
import { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  CircularProgress,
  Alert,
  Grid,
  Card,
  CardContent,
  CardActions,
  Chip,
  Snackbar,
  CardHeader,
  CardActionArea,
  Avatar,
  IconButton
} from '@mui/material';
import { 
  Link as LinkIcon, 
  Sync as SyncIcon, 
  Folder as FolderIcon, 
  Add as AddIcon,
  ArrowForward as ArrowForwardIcon
} from '@mui/icons-material';
import axios from 'axios';
import { useNavigate } from 'react-router-dom';

interface Schema {
  id: number;
  name: string;
  schema: object;
  created_at: string;
}

interface FileInfo {
  name: string;
  path: string;
  size?: number;
  last_modified?: string;
}

interface DatasetSchemaMapping {
  id?: number;
  datasetName: string;
  source: 'local' | 's3';
  schemaId: number | null;
  schemaName: string | null;
  created_at?: string;
  exampleFiles?: Array<string | FileInfo>;
}

interface MappingResponse {
  id: number;
  dataset_name: string;
  source: string;
  schema_id: number | null;
  schema_name: string | null;
  created_at: string;
}

const api = axios.create({
  baseURL: 'http://localhost:5000'
});

export default function DatasetGrid() {
  const [mappings, setMappings] = useState<DatasetSchemaMapping[]>([]);
  const [schemas, setSchemas] = useState<Schema[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [notification, setNotification] = useState('');
  const [applyingSchema, setApplyingSchema] = useState<string | null>(null);
  const [loadingFiles, setLoadingFiles] = useState<Record<string, boolean>>({});
  const navigate = useNavigate();

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    setLoading(true);
    try {
      // Fetch datasets, schemas, and existing mappings in parallel
      const [datasetsResponse, schemasResponse, mappingsResponse] = await Promise.all([
        api.get('/api/datasets'),
        api.get('/api/schemas'),
        api.get('/api/dataset-mappings')
      ]);

      // Process datasets
      const localDatasets = datasetsResponse.data.local || [];
      const s3Datasets = datasetsResponse.data.s3 || [];
      
      // Store schemas
      const fetchedSchemas = schemasResponse.data;
      setSchemas(fetchedSchemas);

      // Process existing mappings
      const existingMappings: MappingResponse[] = mappingsResponse.data;
      const mappingsMap = new Map<string, DatasetSchemaMapping>();
      
      existingMappings.forEach(mapping => {
        const key = `${mapping.source}-${mapping.dataset_name}`;
        mappingsMap.set(key, {
          id: mapping.id,
          datasetName: mapping.dataset_name,
          source: mapping.source as 'local' | 's3',
          schemaId: mapping.schema_id,
          schemaName: mapping.schema_name,
          created_at: mapping.created_at,
          exampleFiles: []
        });
      });

      // Create merged mappings
      const newMappings: DatasetSchemaMapping[] = [];
      
      // Add local datasets
      localDatasets.forEach((name: string) => {
        const key = `local-${name}`;
        if (mappingsMap.has(key)) {
          newMappings.push(mappingsMap.get(key)!);
        } else {
          newMappings.push({
            datasetName: name,
            source: 'local',
            schemaId: null,
            schemaName: null,
            exampleFiles: []
          });
        }
      });
      
      // Add S3 datasets
      s3Datasets.forEach((name: string) => {
        const key = `s3-${name}`;
        if (mappingsMap.has(key)) {
          newMappings.push(mappingsMap.get(key)!);
        } else {
          newMappings.push({
            datasetName: name,
            source: 's3',
            schemaId: null,
            schemaName: null,
            exampleFiles: []
          });
        }
      });

      setMappings(newMappings);
      
      // Fetch example files for each dataset
      newMappings.forEach(mapping => {
        fetchDatasetFiles(mapping);
      });
      
      setError('');
    } catch (err) {
      console.error('Error fetching data:', err);
      setError('Failed to fetch datasets and schemas');
    } finally {
      setLoading(false);
    }
  };

  const fetchDatasetFiles = async (mapping: DatasetSchemaMapping) => {
    const key = `${mapping.source}-${mapping.datasetName}`;
    setLoadingFiles(prev => ({ ...prev, [key]: true }));
    
    try {
      const response = await api.get(`/api/dataset-files/${mapping.source}/${encodeURIComponent(mapping.datasetName)}`);
      
      // Update the mapping with example files
      setMappings(prevMappings => 
        prevMappings.map(m => 
          m.datasetName === mapping.datasetName && m.source === mapping.source
            ? { ...m, exampleFiles: response.data.files.slice(0, 3) } // Take first 3 files as examples
            : m
        )
      );
    } catch (err) {
      console.error(`Error fetching files for dataset ${mapping.datasetName}:`, err);
    } finally {
      setLoadingFiles(prev => ({ ...prev, [key]: false }));
    }
  };

  const handleApplySchema = async (mapping: DatasetSchemaMapping) => {
    if (!mapping.schemaId) return;
    
    const key = `${mapping.source}-${mapping.datasetName}`;
    setApplyingSchema(key);
    
    try {
      const response = await api.post(
        `/api/apply-schema/${mapping.source}/${encodeURIComponent(mapping.datasetName)}`,
        { schema_id: mapping.schemaId }
      );
      
      setNotification(response.data.message || 'Schema applied successfully');
      setTimeout(() => setNotification(''), 3000);
    } catch (err) {
      console.error('Error applying schema:', err);
      setError('Failed to apply schema to dataset');
      setTimeout(() => setError(''), 5000);
    } finally {
      setApplyingSchema(null);
    }
  };

  const handleViewDataset = (mapping: DatasetSchemaMapping) => {
    navigate(`/dataset/${mapping.source}/${encodeURIComponent(mapping.datasetName)}`);
  };

  const handleCreateDataset = () => {
    navigate('/upload');
  };

  const handleRefresh = () => {
    fetchData();
  };

  if (loading) {
    return (
      <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'center', my: 2 }}>
          <CircularProgress />
        </Box>
      </Paper>
    );
  }

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" component="h1">
          Datasets
        </Typography>
        <Box>
          <Button 
            variant="outlined"
            startIcon={<SyncIcon />} 
            onClick={handleRefresh}
            sx={{ mr: 2 }}
          >
            Refresh
          </Button>
          <Button 
            variant="contained" 
            color="primary"
            startIcon={<AddIcon />}
            onClick={handleCreateDataset}
          >
            Add Dataset
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

      {mappings.length === 0 ? (
        <Paper elevation={2} sx={{ p: 4, textAlign: 'center' }}>
          <Typography color="text.secondary" gutterBottom>
            No datasets found
          </Typography>
          <Button 
            variant="contained" 
            color="primary"
            startIcon={<AddIcon />}
            onClick={handleCreateDataset}
            sx={{ mt: 2 }}
          >
            Add Your First Dataset
          </Button>
        </Paper>
      ) : (
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 3 }}>
          {mappings.map((mapping) => {
            const mappingKey = `${mapping.source}-${mapping.datasetName}`;
            const isLoadingFiles = loadingFiles[mappingKey];
            
            return (
              <Box key={mappingKey} sx={{ width: { xs: '100%', sm: 'calc(50% - 12px)', md: 'calc(33.33% - 16px)' } }}>
                <Card elevation={3} sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
                  <CardActionArea onClick={() => handleViewDataset(mapping)}>
                    <CardHeader
                      avatar={
                        <Avatar sx={{ bgcolor: mapping.source === 'local' ? 'info.main' : 'warning.main' }}>
                          <FolderIcon />
                        </Avatar>
                      }
                      title={mapping.datasetName}
                      subheader={
                        <Chip
                          label={mapping.source.toUpperCase()}
                          size="small"
                          color={mapping.source === 'local' ? 'info' : 'warning'}
                          sx={{ mt: 1 }}
                        />
                      }
                    />
                    
                    <CardContent sx={{ flexGrow: 1 }}>
                      <Typography variant="subtitle2" gutterBottom>
                        Schema: {mapping.schemaName || 'None'}
                      </Typography>
                      
                      <Typography variant="subtitle2" gutterBottom sx={{ mt: 2 }}>
                        Example Files:
                      </Typography>
                      
                      {isLoadingFiles ? (
                        <Box sx={{ display: 'flex', justifyContent: 'center', my: 1 }}>
                          <CircularProgress size={20} />
                        </Box>
                      ) : mapping.exampleFiles && mapping.exampleFiles.length > 0 ? (
                        <Box component="ul" sx={{ pl: 2, mt: 1 }}>
                          {mapping.exampleFiles.map((file, index) => (
                            <Typography component="li" variant="body2" key={index}>
                              {typeof file === 'string' ? file : (file.name || file.path)}
                            </Typography>
                          ))}
                        </Box>
                      ) : (
                        <Typography variant="body2" color="text.secondary">
                          No files found
                        </Typography>
                      )}
                    </CardContent>
                  </CardActionArea>
                </Card>
              </Box>
            );
          })}
        </Box>
      )}
    </Box>
  );
} 