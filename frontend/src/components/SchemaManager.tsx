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

type MessageRole = 'user' | 'assistant' | 'system'

interface Message {
  role: MessageRole
  content: string
}

export default function SchemaManager() {
  const [schemas, setSchemas] = useState<Schema[]>([])
  const [loading, setLoading] = useState(false)
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

      {/* Saved Schemas */}
      <Typography variant="subtitle1" gutterBottom>
        Saved Schemas
      </Typography>
      
      <Box sx={{ flexGrow: 1, overflowY: 'auto' }}>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', my: 2 }}>
            <CircularProgress size={24} />
          </Box>
        ) : schemas.length > 0 ? (
          <Box>
            {schemas.map((schema) => (
              <Accordion key={schema.id} sx={{ mb: 1 }} disableGutters>
                <AccordionSummary 
                  expandIcon={<ExpandMoreIcon />}
                  sx={{ minHeight: '48px', py: 0 }}
                >
                  <Typography variant="body2">{schema.name}</Typography>
                  <Chip 
                    size="small" 
                    label={new Date(schema.created_at).toLocaleDateString()} 
                    sx={{ ml: 1, height: '20px', fontSize: '0.7rem' }}
                  />
                </AccordionSummary>
                <AccordionDetails sx={{ p: 1 }}>
                  {editingSchema?.id === schema.id ? (
                    <>
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
                    </>
                  ) : (
                    <>
                      <Box sx={{ maxHeight: 200, overflowY: 'auto' }}>
                        <ReactJson 
                          src={schema.schema} 
                          theme="rjv-default" 
                          displayDataTypes={false}
                          collapsed={2}
                        />
                      </Box>
                      <Box sx={{ mt: 1, display: 'flex', gap: 1 }}>
                        <Button 
                          variant="outlined" 
                          size="small"
                          startIcon={<EditIcon />}
                          onClick={() => handleStartEditing(schema)}
                        >
                          Edit
                        </Button>
                        <Button 
                          variant="outlined" 
                          color="error"
                          size="small"
                          startIcon={<DeleteIcon />}
                          onClick={() => handleDeleteSchema(schema.id)}
                        >
                          Delete
                        </Button>
                      </Box>
                    </>
                  )}
                </AccordionDetails>
              </Accordion>
            ))}
          </Box>
        ) : (
          <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'center', py: 2 }}>
            No schemas found. Generate one using the conversation.
          </Typography>
        )}
      </Box>
    </Box>
  )
} 