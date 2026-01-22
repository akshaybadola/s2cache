import React, { useState, useEffect } from 'react';
import {
  Box, Typography, Grid, Paper, Chip, Card, CardContent, CircularProgress,
  Divider, Accordion, AccordionSummary, AccordionDetails,
  LinearProgress
} from '@mui/material';

import { ExpandMore, AutoStories, ListAlt, Hub, Psychology } from '@mui/icons-material';

import { CollapsibleSummaryGroup } from "../components/CollapsibleMarkdown";

import PaperCard from "../components/PaperCard";
import { PaperDetails } from "../Models";

const BASE_URL = 'http://localhost:9999';

const PaperEntry = ({ paperId, cache, onNavigate }: {
  paperId: string,
  cache: Record<string, PaperDetails>,
  onNavigate: (id: string) => void
}) => {
  const details = cache[paperId];

  if (!details) {
    return (
      <Paper sx={{ p: 2, mb: 2, display: 'flex', alignItems: 'center', gap: 2 }}>
        <CircularProgress size={20} />
        <Typography variant="body2" color="text.secondary">Loading details for {paperId.substring(0, 8)}...</Typography>
      </Paper>
    );
  }

  return <PaperCard paper={details} onNavigate={onNavigate}
           showReferences={false} showCitations={false}
           showAbstract={true}/>;
};

const ResearchStatusView = ({ stateUrl }: { stateUrl: string }) => {
  const [state, setState] = useState<any>(null);
  const [paperCache, setPaperCache] = useState<Record<string, PaperDetails>>({});


  const fetchPaperDetails = async (id: string) => {
      // Skip if already in cache
      if (paperCache[id]) return;

      try {
          const res = await fetch(`${BASE_URL}/s2_details/${id}`);
          const data = await res.json();
          setPaperCache(prev => ({ ...prev, [id]: data }));
      } catch (err) {
          console.error(`Failed to fetch details for ${id}`, err);
      }
  };

  // Poll the state every 5 seconds to show real-time agent progress
  useEffect(() => {
    const fetchState = () => {
      fetch(stateUrl).then(res => {
        console.log(res);
        return res.json()
      }).then(setState);
    };
    fetchState();
    const interval = setInterval(fetchState, 60000);
    return () => clearInterval(interval);
  }, [stateUrl]);

  // 2. Trigger detail fetches whenever state updates
  useEffect(() => {
    if (!state) return;

    // Collect all IDs from various parts of the state
    const allIds = new Set([
      ...state.given_papers,
      ...state.reading_list.map((p: any) => p.paperId),
      ...Object.keys(state.annotations)
    ]);

    // Fetch missing details
    allIds.forEach(id => {
      if (!paperCache[id]) {
        fetchPaperDetails(id);
      }
    });
  }, [state]);

  if (!state) return <LinearProgress />;

  return (
    <Box sx={{ p: 3 }}>
      {/* Header Info */}
      <Box sx={{ mb: 4 }}>
        <Typography variant="h4">{state.given_topic}</Typography>
        <Box sx={{ mt: 1, display: 'flex', gap: 1 }}>
          <Chip label={`Stage: ${state.stage}`} color="primary" variant="outlined" />
          <Chip label={`Hop: ${state.hop}`} color="secondary" variant="outlined" />
          <Chip label={`Processed: ${Object.keys(state.annotations).length} papers`} />
        </Box>
      </Box>

      <Grid container spacing={3}>
        {/* Left Column: Progress & Queues */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2, mb: 3, bgcolor: '#f8f9fa' }}>
            <Typography variant="h6" gutterBottom><Hub /> Discovery Queue</Typography>
            <Typography variant="caption">Pending Analysis: {state.analyze_metadata_list.length}</Typography>
            <LinearProgress
               variant="determinate"
               value={(state.analyze_metadata_list.length / 20) * 100} // Example scale
               sx={{ my: 1 }}
            />
            <Typography variant="caption">Unprocessed Papers: {state.new_papers.length}</Typography>
          </Paper>

          {/* <Grid item xs={12} md={8}> */}
            <Typography variant="h6" gutterBottom><ListAlt /> Seed Papers</Typography>
            <Box sx={{ mb: 4 }}>
              {state.given_papers.map((id: string) => (
                <PaperEntry
                  key={`seed_${id}`}
                  paperId={id}
                  cache={paperCache}
                  onNavigate={(id) => console.log(id)}
                />
              ))}
            </Box>
          </Grid>

        {/* Right Column: Knowledge Base */}
        <Grid item xs={12} md={8}>
          <Typography variant="h5" gutterBottom><Psychology /> Annotated Findings</Typography>

          {Object.entries(state.annotations).map(([id, anno]: [string, any]) => (
            <Card key={id} sx={{ mb: 2, borderLeft: '5px solid #1976d2' }}>
              <CardContent>
                    <Typography variant="body2" sx={{ fontWeight: 'bold' }}>Paper Details</Typography>

                  <AccordionDetails>
                <PaperEntry
                  key={`anno_${anno.paperId}`}
                  paperId={anno.paperId}
                  cache={paperCache}
                  onNavigate={(id) => console.log(id)}
                />
                {/* <Typography variant="h6" color="primary">{anno.paperId}</Typography> */}
                <Typography variant="subtitle2" gutterBottom>Key Idea: {anno.key_idea}</Typography>
                  </AccordionDetails>
                <Accordion size="small" disableGutters elevation={0}>
                  <AccordionSummary expandIcon={<ExpandMore />}>
                    <Typography variant="body2" sx={{ fontWeight: 'bold' }}>Methodology & Results</Typography>
                  </AccordionSummary>
                  <AccordionDetails>
                    <Grid container spacing={2}>
                      <Grid item xs={6}>
                        <Typography variant="caption" display="block" sx={{ fontWeight: 'bold' }}>Steps:</Typography>
                        {Object.entries(anno.method).map(([step, desc]: any) => (
                          <Typography key={step} variant="caption" display="block">{step}. {desc}</Typography>
                        ))}
                      </Grid>
                      <Grid item xs={6}>
                        <Typography variant="caption" display="block" sx={{ fontWeight: 'bold' }}>Results:</Typography>
                        {Object.entries(anno.results).map(([ds, res]: any) => (
                          <Typography key={ds} variant="caption" display="block"><b>{ds}:</b> {res}</Typography>
                        ))}
                      </Grid>
                    </Grid>
                  </AccordionDetails>
                </Accordion>

                <Box sx={{ mt: 1 }}>
                  {anno.datasets.map((d: string) => <Chip key={d} label={d} size="small" sx={{ mr: 0.5 }} />)}
                </Box>
              </CardContent>
            </Card>
          ))}

          <Divider sx={{ my: 4 }} />

          <Typography variant="h6" gutterBottom><AutoStories /> Reading List</Typography>

          {state.reading_list.map((summary: any, idx: number) => {
            return (
              <React.Fragment key={`reading_group_${summary.paperId || idx}`}>
                {/* 1. Rich Paper Metadata */}
                <Box sx={{ mb: 1 }}>
                  <PaperEntry
                    paperId={summary.paperId}
                    cache={paperCache}
                    onNavigate={(id) => console.log(id)}
                  />
                </Box>

                {/* 2. Structured Agent Summary */}
                <Paper sx={{ p: 2, mb: 4, borderRadius: 2 }}>
                  <Typography variant="overline" color="text.secondary" sx={{ ml: 2 }}>
                    AI Generated Analysis
                  </Typography>
                  <CollapsibleSummaryGroup content={summary.summary} />
                </Paper>
              </React.Fragment>
            );
          })}
        </Grid>
      </Grid>
    </Box>
  );
};

export default ResearchStatusView;
