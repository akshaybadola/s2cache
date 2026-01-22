import React, { useState } from 'react';
import {
  Container, Typography, Box,
  TextField, Button, CircularProgress,
  Tabs, Tab, Alert
} from '@mui/material';
import { Search } from '@mui/icons-material';

import { PaperDetails } from "../Models";
import PaperCard from "../components/PaperCard";

const BASE_URL = 'http://localhost:9999';

// --- Main App Component ---
const PaperDetailsView: React.FC = () => {
  const [searchInput, setSearchInput] = useState('');
  const [activeTab, setActiveTab] = useState(0);
  const [history, setHistory] = useState<PaperDetails[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchPaper = async (id: string) => {
    setLoading(true);
    setError(null);
    try {
      // Using your /s2_details/<ssid> route
      const response = await fetch(`${BASE_URL}/s2_details/${id}`);
      const data = await response.json();

      if (data.error) throw new Error(data.message || 'Paper not found');

      // Add to history and switch tab
      setHistory(prev => [...prev, data]);
      setActiveTab(history.length);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (searchInput.trim()) fetchPaper(searchInput.trim());
  };

  return (
    <Container maxWidth="md" sx={{ py: 4 }}>
      <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', textAlign: 'center' }}>
        S2 Paper Explorer
      </Typography>

      {/* Search Bar */}
      <Box component="form" onSubmit={handleSearch} sx={{ display: 'flex', gap: 1, mb: 4 }}>
        <TextField
          fullWidth
          variant="outlined"
          placeholder="Enter ArXiv ID, DOI, or S2 ID (e.g., 1706.03762)"
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
        />
        <Button variant="contained" type="submit" startIcon={<Search />} disabled={loading}>
          Search
        </Button>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      {history.length > 0 && (
        <>
          <Tabs value={activeTab} onChange={(_, v) => setActiveTab(v)} variant="scrollable" scrollButtons="auto">
            {history.map((p, i) => (
              <Tab key={i} label={p.title.substring(0, 20) + '...'} />
            ))}
          </Tabs>
          <Box sx={{ mt: 3 }}>
            <PaperCard
              paper={history[activeTab]}
              onNavigate={(id) => fetchPaper(id)}
              showReferences={true}
              showCitations={true}
              showAbstract={true}
            />
          </Box>
        </>
      )}

      {loading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', mt: 4 }}>
          <CircularProgress />
        </Box>
      )}
    </Container>
  );
};


export default PaperDetailsView;
