import React, { useState, useEffect, useMemo } from 'react';
import {
  Box,
  Typography,
  Grid,
  Paper,
  Chip,
  Card,
  CardContent,
  CircularProgress,
  Divider,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  LinearProgress,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Alert
} from '@mui/material';

import { ExpandMore, AutoStories, ListAlt, Hub, Psychology } from '@mui/icons-material';

import { CollapsibleSummaryGroup } from "../components/CollapsibleMarkdown";

import PaperCard from "../components/PaperCard";
import { PaperDetails } from "../Models";
import { AGENT_URL, BASE_URL } from "../const";

type DiscoveryListItem = {
  id: string;
  name: string;
  path?: string;
  updated_at?: string;
};

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
        <Typography variant="body2" color="text.secondary">
          Loading details for {paperId.substring(0, 8)}...
        </Typography>
      </Paper>
    );
  }

  return (
    <PaperCard
      paper={details}
      onNavigate={onNavigate}
      showReferences={false}
      showCitations={false}
      showAbstract={true}
    />
  );
};

const ResearchStatusView = ({ stateUrl: _stateUrl }: { stateUrl?: string }) => {
  const [discoveries, setDiscoveries] = useState<DiscoveryListItem[]>([]);
  const [selectedDiscovery, setSelectedDiscovery] = useState<string>("");
  const [state, setState] = useState<any>(null);
  const [paperCache, setPaperCache] = useState<Record<string, PaperDetails>>({});
  const [loadingDiscoveries, setLoadingDiscoveries] = useState<boolean>(true);
  const [loadingState, setLoadingState] = useState<boolean>(false);
  const [discoveriesError, setDiscoveriesError] = useState<string>("");
  const [stateError, setStateError] = useState<string>("");

  const discoveryStateUrl = useMemo(() => {
    if (!selectedDiscovery) return null;
    return `${AGENT_URL}/discoveries/${encodeURIComponent(selectedDiscovery)}/state`;
  }, [selectedDiscovery]);

  const fetchPaperDetails = async (id: string) => {
    if (paperCache[id]) return;

    try {
      const res = await fetch(`${BASE_URL}/s2_details/${id}`);
      const data = await res.json();
      setPaperCache(prev => ({ ...prev, [id]: data }));
    } catch (err) {
      console.error(`Failed to fetch details for ${id}`, err);
    }
  };

  useEffect(() => {
    const fetchDiscoveries = async () => {
      setLoadingDiscoveries(true);
      setDiscoveriesError("");

      try {
        const res = await fetch(`${AGENT_URL}/discoveries`);
        if (!res.ok) {
          throw new Error(`Failed to fetch discoveries (${res.status})`);
        }
        const data = await res.json();
        const items: DiscoveryListItem[] = Array.isArray(data?.discoveries) ? data.discoveries : [];
        setDiscoveries(items);

        if (items.length > 0 && !selectedDiscovery) {
          setSelectedDiscovery(items[0].id);
        }
      } catch (err: any) {
        setDiscoveriesError(err?.message || "Failed to fetch discoveries");
      } finally {
        setLoadingDiscoveries(false);
      }
    };

    fetchDiscoveries();
  }, []);

  useEffect(() => {
    if (!discoveryStateUrl) {
      setState(null);
      return;
    }

    const fetchState = async () => {
      setLoadingState(true);
      setStateError("");

      try {
        const res = await fetch(discoveryStateUrl);
        if (!res.ok) {
          throw new Error(`Failed to fetch state (${res.status})`);
        }
        const data = await res.json();
        setState(data);
      } catch (err: any) {
        setStateError(err?.message || "Failed to fetch state");
      } finally {
        setLoadingState(false);
      }
    };

    fetchState();
    const interval = setInterval(fetchState, 60000);
    return () => clearInterval(interval);
  }, [discoveryStateUrl]);

  useEffect(() => {
    if (!state) return;

    const allIds = new Set<string>([
      ...(state.given_papers || []),
      ...((state.reading_list || []).map((p: any) => p.paperId)),
      ...Object.keys(state.annotations || {})
    ]);

    allIds.forEach(id => {
      if (!paperCache[id]) {
        fetchPaperDetails(id);
      }
    });
  }, [state]);

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ mb: 4 }}>
        <Typography variant="h4" gutterBottom>
          Research Discovery Status
        </Typography>

        <Box sx={{ mt: 2, maxWidth: 420 }}>
          <FormControl fullWidth size="small">
            <InputLabel id="discovery-select-label">Discovery</InputLabel>
            <Select
              labelId="discovery-select-label"
              value={selectedDiscovery}
              label="Discovery"
              onChange={(e) => setSelectedDiscovery(e.target.value)}
              disabled={loadingDiscoveries || discoveries.length === 0}
            >
              {discoveries.map((discovery) => (
                <MenuItem key={discovery.id} value={discovery.id}>
                  {discovery.name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Box>

        {loadingDiscoveries && <LinearProgress sx={{ mt: 2 }} />}

        {discoveriesError && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {discoveriesError}
          </Alert>
        )}

        {!loadingDiscoveries && !discoveriesError && discoveries.length === 0 && (
          <Alert severity="info" sx={{ mt: 2 }}>
            No discovery states found.
          </Alert>
        )}
      </Box>

      {loadingState && <LinearProgress sx={{ mb: 3 }} />}

      {stateError && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {stateError}
        </Alert>
      )}

      {!state && !loadingState ? (
        <Alert severity="info">Select a discovery to view its status.</Alert>
      ) : null}

      {state ? (
        <>
          <Box sx={{ mb: 4 }}>
            <Typography variant="h4">{state.given_topic}</Typography>
            <Box sx={{ mt: 1, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
              <Chip label={`Stage: ${state.stage}`} color="primary" variant="outlined" />
              <Chip label={`Hop: ${state.hop}`} color="secondary" variant="outlined" />
              <Chip label={`Processed: ${Object.keys(state.annotations || {}).length} papers`} />
            </Box>
          </Box>

          <Grid container spacing={3}>
            <Grid item xs={12} md={4}>
              <Paper sx={{ p: 2, mb: 3, bgcolor: '#f8f9fa' }}>
                <Typography variant="h6" gutterBottom>
                  <Hub /> Discovery Queue
                </Typography>
                <Typography variant="caption">
                  Pending Analysis: {(state.analyze_metadata_list || []).length}
                </Typography>
                <LinearProgress
                  variant="determinate"
                  value={Math.min(((state.analyze_metadata_list || []).length / 20) * 100, 100)}
                  sx={{ my: 1 }}
                />
                <Typography variant="caption">
                  Unprocessed Papers: {(state.new_papers || []).length}
                </Typography>
              </Paper>

              <Typography variant="h6" gutterBottom>
                <ListAlt /> Seed Papers
              </Typography>
              <Box sx={{ mb: 4 }}>
                {(state.given_papers || []).map((id: string) => (
                  <PaperEntry
                    key={`seed_${id}`}
                    paperId={id}
                    cache={paperCache}
                    onNavigate={(id) => console.log(id)}
                  />
                ))}
              </Box>
            </Grid>

            <Grid item xs={12} md={8}>
              <Typography variant="h5" gutterBottom>
                <Psychology /> Annotated Findings
              </Typography>

              {Object.entries(state.annotations || {}).map(([id, anno]: [string, any]) => (
                <Card key={id} sx={{ mb: 2, borderLeft: '5px solid #1976d2' }}>
                  <CardContent>
                    <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
                      Paper Details
                    </Typography>

                    <AccordionDetails>
                      <PaperEntry
                        key={`anno_${anno.paperId}`}
                        paperId={anno.paperId}
                        cache={paperCache}
                        onNavigate={(paperId) => console.log(paperId)}
                      />
                      <Typography variant="subtitle2" gutterBottom>
                        Key Idea: {anno.key_idea}
                      </Typography>
                    </AccordionDetails>

                    <Accordion disableGutters elevation={0}>
                      <AccordionSummary expandIcon={<ExpandMore />}>
                        <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
                          Methodology & Results
                        </Typography>
                      </AccordionSummary>
                      <AccordionDetails>
                        <Grid container spacing={2}>
                          <Grid item xs={12} sm={6}>
                            <Typography variant="caption" display="block" sx={{ fontWeight: 'bold' }}>
                              Steps:
                            </Typography>
                            {Object.entries(anno.method || {}).map(([step, desc]: any) => (
                              <Typography key={step} variant="caption" display="block">
                                {step}. {desc}
                              </Typography>
                            ))}
                          </Grid>
                          <Grid item xs={12} sm={6}>
                            <Typography variant="caption" display="block" sx={{ fontWeight: 'bold' }}>
                              Results:
                            </Typography>
                            {Object.entries(anno.results || {}).map(([ds, res]: any) => (
                              <Typography key={ds} variant="caption" display="block">
                                <b>{ds}:</b> {res}
                              </Typography>
                            ))}
                          </Grid>
                        </Grid>
                      </AccordionDetails>
                    </Accordion>

                    <Box sx={{ mt: 1, display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                      {(anno.datasets || []).map((d: string) => (
                        <Chip key={d} label={d} size="small" />
                      ))}
                    </Box>
                  </CardContent>
                </Card>
              ))}

              <Divider sx={{ my: 4 }} />

              <Typography variant="h6" gutterBottom>
                <AutoStories /> Reading List
              </Typography>

              {(state.reading_list || []).map((summary: any, idx: number) => {
                return (
                  <React.Fragment key={`reading_group_${summary.paperId || idx}`}>
                    <Box sx={{ mb: 1 }}>
                      <PaperEntry
                        paperId={summary.paperId}
                        cache={paperCache}
                        onNavigate={(id) => console.log(id)}
                      />
                      <CollapsibleSummaryGroup content={summary.summary} />
                    </Box>
                  </React.Fragment>
                );
              })}
            </Grid>
          </Grid>
        </>
      ) : null}
    </Box>
  );
};

export default ResearchStatusView;
