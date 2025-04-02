import { useState } from 'react';
import { Box, Button, TextField, Typography, Paper, List, ListItem, ListItemText, ListItemSecondaryAction, IconButton, LinearProgress } from '@mui/material';
import { CloudUpload as CloudUploadIcon, Delete as DeleteIcon } from '@mui/icons-material';
import { useDropzone } from 'react-dropzone';
import axios from 'axios';

// Changed to keep original file separate from status
interface FileWithStatus {
  file: File;
  id: string; // Unique identifier
  status: 'pending' | 'uploading' | 'success' | 'error';
  progress?: number;
  error?: string;
}

// Configure axios to use the API endpoint
const api = axios.create({
  baseURL: 'http://localhost:5000'
});

export default function FileUpload() {
  const [datasetName, setDatasetName] = useState('');
  const [files, setFiles] = useState<FileWithStatus[]>([]);
  const [error, setError] = useState('');

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    accept: {
      'application/pdf': ['.pdf']
    },
    onDrop: (acceptedFiles) => {
      setFiles(prev => [
        ...prev,
        ...acceptedFiles.map(file => ({
          file: file, // Store the original File object directly
          id: Math.random().toString(36).substring(2, 9), // Generate a simple unique ID
          status: 'pending' as const
        }))
      ]);
      setError('');
    }
  });

  const handleRemoveFile = (fileToRemove: FileWithStatus) => {
    setFiles(files => files.filter(file => file.id !== fileToRemove.id));
  };

  const handleUpload = async (fileItem: FileWithStatus) => {
    if (!datasetName) {
      setError('Please enter a dataset name');
      return;
    }

    const formData = new FormData();
    // Use the original File object directly
    formData.append('file', fileItem.file);
    formData.append('dataset_name', datasetName);

    console.log('Uploading file:', {
      fileName: fileItem.file.name,
      fileSize: fileItem.file.size,
      fileType: fileItem.file.type,
      datasetName: datasetName,
      formDataEntries: Array.from(formData.entries()).map(entry => 
        entry[0] === 'file' ? [entry[0], `[File: ${fileItem.file.name}]`] : entry
      )
    });

    try {
      setFiles(files => files.map(f => 
        f.id === fileItem.id ? { ...f, status: 'uploading' as const, progress: 0 } : f
      ));

      const response = await api.post('/upload', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
        onUploadProgress: (progressEvent) => {
          const progress = progressEvent.loaded / (progressEvent.total || 1) * 100;
          console.log(`Upload progress: ${progress.toFixed(2)}%`);
          setFiles(files => files.map(f => 
            f.id === fileItem.id ? { ...f, progress } : f
          ));
        }
      });

      console.log('Upload response:', response.data);

      setFiles(files => files.map(f => 
        f.id === fileItem.id ? { ...f, status: 'success' as const } : f
      ));
    } catch (err) {
      console.error('Upload error:', err);
      if (axios.isAxiosError(err)) {
        console.error('Axios error details:', {
          status: err.response?.status,
          statusText: err.response?.statusText,
          data: err.response?.data
        });
      }
      setFiles(files => files.map(f => 
        f.id === fileItem.id ? { 
          ...f, 
          status: 'error' as const,
          error: err instanceof Error ? err.message : 'Upload failed'
        } : f
      ));
    }
  };

  const handleUploadAll = async () => {
    if (!datasetName) {
      setError('Please enter a dataset name');
      return;
    }

    const pendingFiles = files.filter(f => f.status === 'pending');
    for (const file of pendingFiles) {
      await handleUpload(file);
    }
  };

  return (
    <Box sx={{ 
      maxWidth: 600, 
      mx: 'auto',
      display: 'flex',
      flexDirection: 'column',
      gap: 2
    }}>
      <Typography variant="h6" align="center" gutterBottom>
        Upload PDF Files
      </Typography>
      
      <TextField
        fullWidth
        label="Dataset Name"
        value={datasetName}
        onChange={(e) => setDatasetName(e.target.value)}
        margin="normal"
        required
      />

      <Paper
        {...getRootProps()}
        sx={{
          p: 4,
          mt: 1,
          mb: 2,
          textAlign: 'center',
          backgroundColor: isDragActive ? 'action.hover' : 'background.paper',
          cursor: 'pointer',
          border: '2px dashed',
          borderColor: isDragActive ? 'primary.main' : 'divider',
          borderRadius: 2,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: 2
        }}
      >
        <input {...getInputProps()} />
        <CloudUploadIcon sx={{ fontSize: 48, color: 'primary.main' }} />
        <Typography>
          {isDragActive
            ? 'Drop PDF files here'
            : 'Drag and drop PDF files here, or click to select'}
        </Typography>
      </Paper>

      {files.length > 0 && (
        <Paper sx={{ mt: 2 }}>
          <List>
            {files.map((fileItem, index) => (
              <ListItem key={fileItem.id} divider={index < files.length - 1}>
                <ListItemText
                  primary={fileItem.file.name}
                  secondary={`${(fileItem.file.size / 1024 / 1024).toFixed(2)} MB`}
                />
                <ListItemSecondaryAction>
                  {fileItem.status === 'pending' && (
                    <Button
                      variant="contained"
                      size="small"
                      onClick={() => handleUpload(fileItem)}
                    >
                      Upload
                    </Button>
                  )}
                  {fileItem.status === 'uploading' && (
                    <Box sx={{ width: 200 }}>
                      <LinearProgress 
                        variant="determinate" 
                        value={fileItem.progress || 0} 
                      />
                      <Typography variant="caption" color="text.secondary">
                        {Math.round(fileItem.progress || 0)}%
                      </Typography>
                    </Box>
                  )}
                  {fileItem.status === 'success' && (
                    <Typography color="success.main">
                      Uploaded
                    </Typography>
                  )}
                  {fileItem.status === 'error' && (
                    <Typography color="error">
                      {fileItem.error}
                    </Typography>
                  )}
                  <IconButton
                    edge="end"
                    aria-label="delete"
                    onClick={() => handleRemoveFile(fileItem)}
                    sx={{ ml: 1 }}
                  >
                    <DeleteIcon />
                  </IconButton>
                </ListItemSecondaryAction>
              </ListItem>
            ))}
          </List>
        </Paper>
      )}

      {files.length > 0 && (
        <Button
          variant="contained"
          color="primary"
          size="large"
          onClick={handleUploadAll}
          disabled={!datasetName || files.every(f => f.status !== 'pending')}
          sx={{ mt: 1 }}
        >
          Upload All Pending Files
        </Button>
      )}

      {error && (
        <Typography color="error" align="center" sx={{ mt: 2 }}>
          {error}
        </Typography>
      )}
    </Box>
  );
} 