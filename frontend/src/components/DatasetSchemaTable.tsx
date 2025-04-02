import { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
  Button,
  CircularProgress,
  Alert,
  Select,
  MenuItem,
  FormControl,
  SelectChangeEvent,
  Snackbar
} from '@mui/material';
import { Link as LinkIcon, Sync as SyncIcon } from '@mui/icons-material';
import axios from 'axios';

interface Schema {
  id: number;
  name: string;
  schema: object;
  created_at: string;
}

interface DatasetSchemaMapping {
  id?: number;
  datasetName: string;
  source: 'local' | 's3';
  schemaId: number | null;
  schemaName: string | null;
  created_at?: string;
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

export default function DatasetSchemaTable() {
  const [mappings, setMappings] = useState<DatasetSchemaMapping[]>([]);
  const [schemas, setSchemas] = useState<Schema[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [notification, setNotification] = useState('');
  const [applyingSchema, setApplyingSchema] = useState<string | null>(null);

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
          created_at: mapping.created_at
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
            schemaName: null
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
            schemaName: null
          });
        }
      });

      setMappings(newMappings);
      setError('');
    } catch (err) {
      console.error('Error fetching data:', err);
      setError('Failed to fetch datasets and schemas');
    } finally {
      setLoading(false);
    }
  };

  const handleSchemaChange = async (datasetIndex: number, schemaId: number | null) => {
    const mapping = mappings[datasetIndex];
    const newMappings = [...mappings];
    
    newMappings[datasetIndex] = {
      ...mapping,
      schemaId: schemaId,
      schemaName: schemaId 
        ? schemas.find(s => s.id === schemaId)?.name || null 
        : null
    };
    
    setMappings(newMappings);
    
    try {
      await api.post('/api/dataset-mappings', {
        dataset_name: mapping.datasetName,
        source: mapping.source,
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
    <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="h5" component="h2">
          Dataset Schema Mappings
        </Typography>
        <Button 
          startIcon={<SyncIcon />} 
          onClick={handleRefresh}
          size="small"
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

      {mappings.length === 0 ? (
        <Typography color="text.secondary" align="center" sx={{ py: 2 }}>
          No datasets found. Upload files to create datasets.
        </Typography>
      ) : (
        <TableContainer sx={{ maxHeight: 300 }}>
          <Table stickyHeader size="small">
            <TableHead>
              <TableRow>
                <TableCell><strong>Dataset Name</strong></TableCell>
                <TableCell><strong>Source</strong></TableCell>
                <TableCell><strong>Schema</strong></TableCell>
                <TableCell><strong>Actions</strong></TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {mappings.map((mapping, index) => {
                const mappingKey = `${mapping.source}-${mapping.datasetName}`;
                const isApplying = applyingSchema === mappingKey;
                
                return (
                  <TableRow key={mappingKey}>
                    <TableCell>{mapping.datasetName}</TableCell>
                    <TableCell>
                      <Box sx={{ 
                        display: 'inline-block', 
                        bgcolor: mapping.source === 'local' ? 'info.light' : 'warning.light',
                        color: mapping.source === 'local' ? 'info.contrastText' : 'warning.contrastText',
                        px: 1,
                        py: 0.5,
                        borderRadius: 1,
                        fontSize: '0.75rem'
                      }}>
                        {mapping.source.toUpperCase()}
                      </Box>
                    </TableCell>
                    <TableCell>
                      <FormControl fullWidth size="small">
                        <Select
                          value={mapping.schemaId?.toString() || ''}
                          onChange={(e: SelectChangeEvent) => {
                            const value = e.target.value;
                            handleSchemaChange(index, value ? Number(value) : null);
                          }}
                          displayEmpty
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
                    </TableCell>
                    <TableCell>
                      <Button 
                        size="small" 
                        startIcon={isApplying ? <CircularProgress size={16} /> : <LinkIcon />}
                        disabled={!mapping.schemaId || isApplying}
                        onClick={() => handleApplySchema(mapping)}
                        variant="outlined"
                      >
                        Apply
                      </Button>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Paper>
  );
} 