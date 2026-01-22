import React, { useState } from 'react';
import {
  Box, TextField, Button, Typography, List, ListItem,
  ListItemText, ListItemButton, Paper, Divider, IconButton
} from '@mui/material';
import { Delete, Search, PlayArrow, Science } from '@mui/icons-material';

const BASE_URL = 'http://localhost:9999';

const ResearchDiscoveryView = () => {
  const [topic, setTopic] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [seedPapers, setSeedPapers] = useState<any[]>([]);
  const [isProcessing, setIsProcessing] = useState(false);

  // Search for papers to add to the "Given Papers" list
  const handleSearch = async () => {
      if (!searchQuery) return;
      try {
          const res = await fetch(`${BASE_URL}/s2_details/${searchQuery}`);
          const data = await res.json();

          // Use spread (...) to flatten the data into the results array
          if (Array.isArray(data) && data.length !== 0) {
              setSearchResults(data); // Or [...searchResults, ...data] if appending
          } else if (data && !Array.isArray(data)) {
              // If the API returns a single object instead of an array
              setSearchResults([data]);
          }
      } catch (error) {
          console.error("Search failed", error);
      }
  };

  const addPaper = (paper: any) => {
    if (seedPapers.find(p => p.paperId === paper.paperId)) return;
    setSeedPapers([...seedPapers, paper]);
    setSearchResults([]);
    setSearchQuery('');
  };

  const removePaper = (id: string) => {
    setSeedPapers(seedPapers.filter(p => p.paperId !== id));
  };

  const startDiscovery = async () => {
    setIsProcessing(true);
    try {
      // payload matches your AlgoResearchDiscovery config expectations
      const payload = {
        topic: topic,
        given_papers: seedPapers.map(p => p.paperId),
        max_hops: 2,
        new_paper_limit: 16
      };

      const res = await fetch(`${BASE_URL}/start_discovery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      // Handle response / navigate to status page
    } finally {
      setIsProcessing(false);
    }
  };

  return (
    <Box sx={{ maxWidth: 800, mx: 'auto', p: 3 }}>
      <Typography variant="h4" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
        <Science color="primary" /> Research Discovery
      </Typography>

      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h6" gutterBottom>1. Define Discovery Goal</Typography>
        <TextField
          fullWidth
          label="Research Topic / Objective"
          placeholder="e.g., Latent Diffusion Models for Medical Imaging"
          value={topic}
          onChange={(e) => setTopic(e.target.value)}
          sx={{ mb: 3 }}
        />

        <Typography variant="h6" gutterBottom>2. Add Seed Papers ({seedPapers.length})</Typography>
        <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
          <TextField
            fullWidth
            size="small"
            placeholder="Search for initial papers..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
          />
          <Button variant="contained" onClick={handleSearch}><Search /></Button>
        </Box>

        {/* Search Results Dropdown */}
        {searchResults.length > 0 && (
          <Paper variant="outlined" sx={{ mb: 2, maxHeight: 300, overflow: 'auto' }}>
            <List dense>
              {searchResults.map((p) => (
                <ListItem key={p.paperId} disablePadding>
                  <ListItemButton onClick={() => addPaper(p)}>
                    <ListItemText primary={p.title} secondary={p.year} />
                  </ListItemButton>
                </ListItem>
              ))}
            </List>
          </Paper>
        )}

        {/* Selected Papers List */}
        <List>
          {seedPapers.map((p) => (
            <ListItem
              key={p.paperId}
              secondaryAction={
                <IconButton edge="end" onClick={() => removePaper(p.paperId)}>
                  <Delete />
                </IconButton>
              }
              sx={{ bgcolor: 'action.hover', mb: 1, borderRadius: 1 }}
            >
              {/* Ensure authors exists before accessing */}
              <ListItemText
                primary={p.title}
                secondary={`${p.authors?.[0]?.name || 'Unknown Author'} • ${p.year}`}
              />
            </ListItem>
          ))}
        </List>

        <Divider sx={{ my: 3 }} />

        <Button
          fullWidth
          variant="contained"
          size="large"
          disabled={!topic || seedPapers.length === 0 || isProcessing}
          startIcon={<PlayArrow />}
          onClick={startDiscovery}
        >
          {isProcessing ? 'Initializing Algorithm...' : 'Start Research Discovery'}
        </Button>
      </Paper>
    </Box>
  );
};

export default ResearchDiscoveryView;
