import React, { useState, useEffect, useRef, useCallback, memo } from 'react';
import {
  Typography, Box, IconButton,  List, ListItem,
  Collapse, CircularProgress,
} from '@mui/material';
import { ExpandMore, ExpandLess, OpenInNew } from '@mui/icons-material';

const BASE_URL = 'http://localhost:9999';

// --- Optimized Individual Item Component ---
// Memoizing this prevents the "weird lag" because the list only re-renders
// the specific item that changed.
const PaperListItem = memo(({ item, onNavigate }: { item: any, onNavigate: (id: string) => void }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const p = item.citingPaper || item.citedPaper || item;

  if (!p?.title) return null;

  return (
    <ListItem
      divider={!isExpanded}
      sx={{
        flexDirection: 'column',
        alignItems: 'stretch',
        bgcolor: isExpanded ? 'action.hover' : 'transparent',
        transition: 'background-color 0.2s'
      }}
    >
      <Box sx={{ display: 'flex', width: '100%', alignItems: 'center', py: 0.5 }}>
        <Box
          sx={{ flex: 1, cursor: 'pointer' }}
          onClick={() => setIsExpanded(!isExpanded)}
        >
          <Typography variant="body2" sx={{ fontWeight: 600, lineHeight: 1.2 }}>
            {p.title}
          </Typography>
          <Typography variant="caption" color="text.secondary">
            {p.authors?.[0]?.name || 'Unknown'} et al. • {p.year || 'N/A'}
          </Typography>
        </Box>

        <Box>
          <IconButton size="small" onClick={() => onNavigate(p.paperId)} title="Explore Paper">
            <OpenInNew fontSize="inherit" />
          </IconButton>
          <IconButton size="small" onClick={() => setIsExpanded(!isExpanded)}>
            {isExpanded ? <ExpandLess fontSize="inherit" /> : <ExpandMore fontSize="inherit" />}
          </IconButton>
        </Box>
      </Box>

      <Collapse in={isExpanded} timeout="auto" unmountOnExit>
        <Box sx={{
          p: 2, mt: 1, mb: 1,
          bgcolor: 'background.paper',
          borderRadius: 1,
          border: '1px solid',
          borderColor: 'divider',
          boxShadow: 'inset 0px 2px 4px rgba(0,0,0,0.05)'
        }}>
          {p.abstract ? (
            <Typography variant="body2" sx={{ fontStyle: 'italic', mb: 1 }}>
              {p.abstract}
            </Typography>
          ) : (
            <Typography variant="caption" color="text.secondary">No abstract available.</Typography>
          )}
          <Box sx={{
            display: 'flex',
            gap: 1,
            mt: 1,
            flexWrap: 'wrap'
          }}>
            {p.venue && <Box sx={{
              fontSize: '10px',
              px: 1,
              bgcolor: 'grey.200',
              borderRadius: 1
            }}>{p.venue}</Box>}
            {p.externalIds?.DOI &&
              <Box
                component="a"
                href={`https://doi.org/${p.externalIds.DOI}`}
                target="_blank"
                rel="noopener noreferrer"
                sx={{
                  fontSize: '10px',
                  px: 1,
                  bgcolor: 'blue.50',
                  color: 'primary.main',
                  borderRadius: 1
                }}>DOI: {p.externalIds.DOI}</Box>}
            {p.externalIds?.ArXiv &&
              <Box
                component="a"
                href={`https://www.arxiv.org/abs/${p.externalIds.ArXiv}`}
                target="_blank"
                rel="noopener noreferrer"
                sx={{
                  fontSize: '10px',
                  px: 1,
                  bgcolor: 'blue.50',
                  color: 'primary.main',
                  borderRadius: 1
                }}>ArXiv: {p.externalIds.ArXiv}</Box>}
            {p.externalIds?.DBLP &&
              <Box
                component="a"
                href={`https://dblp.org/rec/${p.externalIds.DBLP}`}
                target="_blank"
                rel="noopener noreferrer"
                sx={{
                  fontSize: '10px',
                  px: 1,
                  bgcolor: 'blue.50',
                  color: 'primary.main',
                  borderRadius: 1
                }}>DBLP: {p.externalIds.DBLP}</Box>}
           </Box>
        </Box>
      </Collapse>
    </ListItem>
  );
});

// --- Main List Component ---
const InfiniteList: React.FC<{
  type: 'citations' | 'references',
  ssid: string,
  onNavigate: (id: string) => void,
  initialItems: any[]
}> = ({ type, ssid, onNavigate, initialItems }) => {
  const [items, setItems] = useState<any[]>(initialItems);
  const [offset, setOffset] = useState(initialItems.length);
  const [hasMore, setHasMore] = useState(true);
  const [loading, setLoading] = useState(false);
  const observerTarget = useRef(null);

  // Use a ref for offset to avoid unnecessary re-renders in the observer
  const offsetRef = useRef(initialItems.length);

  const fetchData = useCallback(async () => {
    if (loading || !hasMore) return;
    setLoading(true);
    try {
      const response = await fetch(`${BASE_URL}/s2_${type}/${ssid}?offset=${offsetRef.current}&count=100`);
      const result = await response.json();

      if (result.data?.length > 0) {
        setItems(prev => [...prev, ...result.data]);
        offsetRef.current += result.data.length;
        setOffset(offsetRef.current);
        setHasMore(result.next !== null);
      } else {
        setHasMore(false);
      }
    } catch (err) {
      setHasMore(false);
    } finally {
      setLoading(false);
    }
  }, [ssid, type, loading, hasMore]);

  useEffect(() => {
    const observer = new IntersectionObserver(
      entries => { if (entries[0].isIntersecting) fetchData(); },
      { threshold: 0.1 }
    );
    if (observerTarget.current) observer.observe(observerTarget.current);
    return () => observer.disconnect();
  }, [fetchData]);

  return (
    <List dense disablePadding>
      {items.map((item, i) => (
        <PaperListItem
          key={`${ssid}-${type}-${i}`} // Stable unique key
          item={item}
          onNavigate={onNavigate}
        />
      ))}
      <Box ref={observerTarget} sx={{ py: 4, display: 'flex', justifyContent: 'center' }}>
        {loading && <CircularProgress size={24} />}
        {!hasMore && <Typography variant="caption">All items loaded</Typography>}
      </Box>
    </List>
  );
};

export default InfiniteList;
