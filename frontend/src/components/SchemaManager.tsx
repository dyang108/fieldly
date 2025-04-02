import { useState, useEffect } from 'react'
import {
  Box,
  Button,
  TextField,
  Typography,
  Alert,
  Card,
  CardContent,
  CircularProgress,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Chip,
  Snackbar
} from '@mui/material'
import { Delete as DeleteIcon, Edit as EditIcon, ExpandMore as ExpandMoreIcon, Refresh as RefreshIcon } from '@mui/icons-material'
import axios from 'axios'
// @ts-ignore
import ReactJson from 'react-json-view'

// Configure axios to use the API endpoint
const api = axios.create({
  baseURL: 'http://localhost:5000'
})

interface Schema {
  id: number
  name: string
  schema: object
  created_at: string
}

interface DatasetMapping {
  id: number
  dataset_name: string
  source: string
  schema_id: number
  schema_name: string
  created_at: string
}

type MessageRole = 'user' | 'assistant' | 'system'

interface Message {
  role: MessageRole
  content: string
}

export default function SchemaManager() {
  const [schemas, setSchemas] = useState<Schema[]>([])
  const [datasetMappings, setDatasetMappings] = useState<DatasetMapping[]>([])
  const [loading, setLoading] = useState(false)
  const [extracting, setExtracting] = useState(false)
  const [extractionResult, setExtractionResult] = useState<any>(null)
  const [error, setError] = useState('')
  const [notification, setNotification] = useState('')
  
  // Conversation state
  const [userPrompt, setUserPrompt] = useState('')
  const [conversation, setConversation] = useState<Message[]>([])
  const [generatedSchema, setGeneratedSchema] = useState<object | null>(null)
  const [generatingSchema, setGeneratingSchema] = useState(false)
  const [schemaName, setSchemaName] = useState('')

  // Edit state
  const [editingSchema, setEditingSchema] = useState<Schema | null>(null)
  const [editedSchema, setEditedSchema] = useState<object | null>(null)

  useEffect(() => {
    fetchSchemas()
    fetchDatasetMappings()
  }, [])

  const fetchSchemas = async () => {
    setLoading(true)
    try {
      const response = await api.get('/api/schemas')
      setSchemas(response.data)
      setError('')
    } catch (err) {
      console.error('Error fetching schemas:', err)
      setError('Failed to fetch schemas')
    } finally {
      setLoading(false)
    }
  }

  const fetchDatasetMappings = async () => {
    try {
      const response = await api.get('/api/dataset-mappings')
      setDatasetMappings(response.data)
    } catch (err) {
      console.error('Error fetching dataset mappings:', err)
    }
  }

  const handleSendPrompt = async () => {
    if (!userPrompt.trim()) return
    
    // Add user message to conversation
    const userMessage: Message = { role: 'user', content: userPrompt }
    const updatedConversation = [...conversation, userMessage]
    setConversation(updatedConversation)
    
    // Clear prompt field
    setUserPrompt('')
    
    // Generate schema from conversation
    setGeneratingSchema(true)
    try {
      const response = await api.post('/api/generate-schema', {
        conversation: updatedConversation
      })
      
      // Add assistant response to conversation
      const assistantMessage: Message = { 
        role: 'assistant', 
        content: response.data.message || 'Here is the generated schema.'
      }
      setConversation([...updatedConversation, assistantMessage])
      
      // Set generated schema
      setGeneratedSchema(response.data.schema)
      
      // Default schema name if empty
      if (!schemaName) {
        setSchemaName(response.data.suggested_name || 'New Schema')
      }
      
    } catch (err) {
      console.error('Error generating schema:', err)
      setError('Failed to generate schema')
      // Add error message to conversation
      const errorMessage: Message = {
        role: 'assistant',
        content: 'Sorry, I encountered an error generating the schema.'
      }
      setConversation([...updatedConversation, errorMessage])
    } finally {
      setGeneratingSchema(false)
    }
  }

  const handleSaveGeneratedSchema = async () => {
    if (!generatedSchema) return
    if (!schemaName.trim()) {
      setError('Please provide a name for the schema')
      return
    }

    try {
      await api.post('/api/schemas', {
        name: schemaName,
        schema: generatedSchema
      })
      
      // Reset conversation and generated schema
      setConversation([])
      setGeneratedSchema(null)
      setSchemaName('')
      
      // Show success notification
      setNotification('Schema saved successfully')
      setTimeout(() => setNotification(''), 3000)
      
      // Refresh schemas list
      fetchSchemas()
    } catch (err) {
      console.error('Error saving schema:', err)
      setError('Failed to save schema')
    }
  }

  const handleStartEditing = (schema: Schema) => {
    setEditingSchema(schema)
    setEditedSchema(schema.schema)
  }

  const handleUpdateSchema = async () => {
    if (!editingSchema || !editedSchema) return

    try {
      await api.put(`/api/schemas/${editingSchema.id}`, {
        schema: editedSchema
      })
      
      // Reset editing state
      setEditingSchema(null)
      setEditedSchema(null)
      
      // Show success notification
      setNotification('Schema updated successfully')
      setTimeout(() => setNotification(''), 3000)
      
      // Refresh schemas list
      fetchSchemas()
    } catch (err) {
      console.error('Error updating schema:', err)
      setError('Failed to update schema')
    }
  }

  const handleDeleteSchema = async (id: number) => {
    try {
      await api.delete(`/api/schemas/${id}`)
      
      // Show success notification
      setNotification('Schema deleted successfully')
      setTimeout(() => setNotification(''), 3000)
      
      // Refresh schemas list
      fetchSchemas()
    } catch (err) {
      console.error('Error deleting schema:', err)
      setError('Failed to delete schema')
    }
  }

  const handleResetConversation = () => {
    setConversation([])
    setGeneratedSchema(null)
    setSchemaName('')
  }

  const handleExtractData = async (datasetName: string, source: string) => {
    setExtracting(true)
    setExtractionResult(null)
    try {
      const response = await api.post(`/api/extract/${source}/${datasetName}`)
      setExtractionResult(response.data)
      setNotification(`Data extraction completed for ${datasetName}`)
    } catch (err) {
      console.error('Error extracting data:', err)
      setError('Failed to extract data from dataset')
    } finally {
      setExtracting(false)
    }
  }

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {notification && (
        <Snackbar
          open={!!notification}
          autoHideDuration={3000}
          message={notification}
          onClose={() => setNotification('')}
        />
      )}
      
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {/* Chat interface */}
      <Box sx={{ mb: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
          <Typography variant="subtitle1">
            Generate Schema
          </Typography>
          {conversation.length > 0 && (
            <Button 
              size="small" 
              startIcon={<RefreshIcon />} 
              onClick={handleResetConversation}
            >
              Reset
            </Button>
          )}
        </Box>
        
        {/* Conversation Display */}
        {conversation.length > 0 && (
          <Box sx={{ mb: 2, maxHeight: 200, overflowY: 'auto', p: 1 }}>
            {conversation.map((message, index) => (
              <Box 
                key={index} 
                sx={{
                  display: 'flex',
                  justifyContent: message.role === 'user' ? 'flex-end' : 'flex-start',
                  mb: 1
                }}
              >
                <Card 
                  sx={{ 
                    maxWidth: '80%',
                    bgcolor: message.role === 'user' ? 'primary.light' : 'grey.100'
                  }}
                >
                  <CardContent sx={{ py: 1, px: 1.5, '&:last-child': { pb: 1 } }}>
                    <Typography variant="body2" color={message.role === 'user' ? 'white' : 'text.primary'}>
                      {message.content}
                    </Typography>
                  </CardContent>
                </Card>
              </Box>
            ))}
          </Box>
        )}
        
        {/* Input area */}
        <Box sx={{ display: 'flex', gap: 1 }}>
          <TextField
            size="small"
            fullWidth
            placeholder="Describe your schema..."
            value={userPrompt}
            onChange={(e) => setUserPrompt(e.target.value)}
            onKeyPress={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                handleSendPrompt()
                e.preventDefault()
              }
            }}
          />
          <Button
            variant="contained"
            color="primary"
            size="small"
            onClick={handleSendPrompt}
            disabled={generatingSchema || !userPrompt.trim()}
            sx={{ minWidth: '80px' }}
          >
            {generatingSchema ? <CircularProgress size={24} /> : 'Send'}
          </Button>
        </Box>
      </Box>

      {/* Generated Schema Preview */}
      {generatedSchema && (
        <Box sx={{ mb: 2 }}>
          <Typography variant="subtitle1" gutterBottom>
            Schema Preview:
          </Typography>
          <TextField
            fullWidth
            size="small"
            label="Schema Name"
            value={schemaName}
            onChange={(e) => setSchemaName(e.target.value)}
            margin="dense"
            required
          />
          <Box sx={{ mt: 1, maxHeight: 200, overflowY: 'auto', border: '1px solid #eee', borderRadius: 1 }}>
            <ReactJson 
              src={generatedSchema} 
              theme="rjv-default" 
              displayDataTypes={false}
              collapsed={1}
              style={{ padding: '8px' }}
            />
          </Box>
          <Button
            variant="contained"
            color="primary"
            size="small"
            sx={{ mt: 1 }}
            onClick={handleSaveGeneratedSchema}
          >
            Save Schema
          </Button>
        </Box>
      )}

      {/* Existing Schemas */}
      <Typography variant="h6" sx={{ mt: 2, mb: 1 }}>
        Schemas
        <Button 
          size="small" 
          startIcon={<RefreshIcon />} 
          onClick={fetchSchemas}
          sx={{ ml: 2 }}
        >
          Refresh
        </Button>
      </Typography>

      {loading ? (
        <CircularProgress />
      ) : schemas.length === 0 ? (
        <Typography color="text.secondary">No schemas found</Typography>
      ) : (
        schemas.map((schema) => (
          <Accordion key={schema.id} sx={{ mb: 1 }}>
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%', pr: 2 }}>
                <Typography>{schema.name}</Typography>
                <Box sx={{ display: 'flex', ml: 2 }}>
                  <Button 
                    size="small" 
                    startIcon={<EditIcon />} 
                    onClick={(e) => {
                      e.stopPropagation();
                      handleStartEditing(schema);
                    }}
                    sx={{ mr: 1 }}
                  >
                    Edit
                  </Button>
                  <Button 
                    size="small" 
                    color="error"
                    startIcon={<DeleteIcon />} 
                    onClick={(e) => {
                      e.stopPropagation();
                      if (window.confirm(`Delete schema ${schema.name}?`)) {
                        handleDeleteSchema(schema.id);
                      }
                    }}
                  >
                    Delete
                  </Button>
                </Box>
              </Box>
            </AccordionSummary>
            <AccordionDetails>
              {/* Schema content */}
              <ReactJson 
                src={schema.schema} 
                name={false} 
                collapsed={1}
                displayDataTypes={false}
                enableClipboard={false}
              />
              
              {/* Associated Datasets */}
              <Box sx={{ mt: 2 }}>
                <Typography variant="subtitle2">Associated Datasets:</Typography>
                {datasetMappings.filter(mapping => mapping.schema_id === schema.id).length === 0 ? (
                  <Typography variant="body2" color="text.secondary">No datasets using this schema</Typography>
                ) : (
                  datasetMappings
                    .filter(mapping => mapping.schema_id === schema.id)
                    .map(mapping => (
                      <Box key={mapping.id} sx={{ display: 'flex', alignItems: 'center', mt: 1 }}>
                        <Chip 
                          label={`${mapping.dataset_name} (${mapping.source})`} 
                          size="small" 
                          sx={{ mr: 1 }}
                        />
                        <Button 
                          size="small" 
                          variant="outlined" 
                          color="primary"
                          disabled={extracting}
                          onClick={() => handleExtractData(mapping.dataset_name, mapping.source)}
                        >
                          {extracting ? 'Extracting...' : 'Extract Data'}
                        </Button>
                      </Box>
                    ))
                )}
              </Box>
            </AccordionDetails>
          </Accordion>
        ))
      )}

      {/* Editing Modal */}
      {editingSchema && (
        <Box sx={{ mt: 2 }}>
          <Typography variant="h6" gutterBottom>
            Edit Schema
          </Typography>
          <Box sx={{ maxHeight: 200, overflowY: 'auto' }}>
            <ReactJson 
              src={editedSchema || {}} 
              theme="rjv-default" 
              displayDataTypes={false}
              onEdit={(edit: any) => setEditedSchema(edit.updated_src)}
              onAdd={(add: any) => setEditedSchema(add.updated_src)}
              onDelete={(del: any) => setEditedSchema(del.updated_src)}
            />
          </Box>
          <Box sx={{ mt: 1, display: 'flex', gap: 1 }}>
            <Button 
              variant="contained" 
              color="primary"
              size="small"
              onClick={handleUpdateSchema}
            >
              Save
            </Button>
            <Button 
              variant="outlined"
              size="small"
              onClick={() => setEditingSchema(null)}
            >
              Cancel
            </Button>
          </Box>
        </Box>
      )}

      {/* Extraction Results */}
      {extractionResult && (
        <Box sx={{ mt: 2 }}>
          <Typography variant="h6">Extraction Results</Typography>
          <Card sx={{ mt: 1 }}>
            <CardContent>
              <Typography variant="subtitle1" color="primary">
                Dataset: {extractionResult.dataset}
              </Typography>
              <Typography variant="body2">
                Output Directory: {extractionResult.output_directory}
              </Typography>
              <Typography variant="body2">
                Processed {extractionResult.processed_files} files
              </Typography>
              
              <Typography variant="subtitle2" sx={{ mt: 2 }}>Results:</Typography>
              <Box sx={{ mt: 1, maxHeight: 200, overflowY: 'auto' }}>
                {extractionResult.results.map((result: any, index: number) => (
                  <Alert 
                    key={index} 
                    severity={result.status === 'success' ? 'success' : 'error'}
                    sx={{ mb: 1 }}
                  >
                    <Typography variant="body2">
                      {result.filename}: {result.status === 'success' 
                        ? `Extracted to ${result.output_file}` 
                        : result.message}
                    </Typography>
                  </Alert>
                ))}
              </Box>
            </CardContent>
          </Card>
        </Box>
      )}
    </Box>
  )
} 