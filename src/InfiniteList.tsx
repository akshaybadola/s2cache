import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Typography, Box, IconButton,  List, ListItem,
  Collapse, CircularProgress,
} from '@mui/material';
import { ExpandMore, ExpandLess, OpenInNew } from '@mui/icons-material';

const BASE_URL = 'http://localhost:9999';

const InfiniteList: React.FC<{
  type: 'citations' | 'references',
  ssid: string,
  onNavigate: (id: string) => void,
  initialItems: any[]
}> = ({ type, ssid, onNavigate, initialItems }) => {
  const [items, setItems] = useState<any[]>(initialItems);
  const [offset, setOffset] = useState(initialItems.length);
  const [hasMore, setHasMore] = useState(initialItems.length > 0);
  const [loading, setLoading] = useState(false);
  const [expandedItem, setExpandedItem] = useState<string | null>(null); // Track which list item is open
  const observerTarget = useRef(null);

  const fetchData = useCallback(async () => {
    if (loading || !hasMore) return;
    setLoading(true);
    try {
      const response = await fetch(`${BASE_URL}/s2_${type}/${ssid}?offset=${offset}&count=100`);
      const result = await response.json();
      if (result.data?.length > 0) {
        setItems(prev => [...prev, ...result.data]);
        setOffset(prev => prev + result.data.length);
        setHasMore(result.next !== null);
      } else { setHasMore(false); }
    } catch (err) { setHasMore(false); } finally { setLoading(false); }
  }, [offset, ssid, type, loading, hasMore]);

  useEffect(() => {
    const observer = new IntersectionObserver(
      entries => { if (entries[0].isIntersecting) fetchData(); },
      { threshold: 0.1 }
    );
    if (observerTarget.current) observer.observe(observerTarget.current);
    return () => observer.disconnect();
  }, [fetchData]);

  return (
    <List dense>
      {items.map((item, i) => {
        const p = item.citingPaper || item.citedPaper || item;
        if (!p?.title) return null;
        const isItemExpanded = expandedItem === `${p.paperId}-${i}`;

        return (
          <React.Fragment key={`${p.paperId}-${i}`}>
            <ListItem
              divider={!isItemExpanded}
              sx={{
                flexDirection: 'column',
                alignItems: 'stretch',
                bgcolor: isItemExpanded ? 'action.selected' : 'transparent'
              }}
            >
              <Box sx={{ display: 'flex', width: '100%', alignItems: 'center', py: 1 }}>
                <Box
                  sx={{ flex: 1, cursor: 'pointer' }}
                  onClick={() => setExpandedItem(isItemExpanded ? null : `${p.paperId}-${i}`)}
                >
                  <Typography variant="body2" sx={{ fontWeight: 600 }}>{p.title}</Typography>
                  <Typography variant="caption" color="text.secondary">
                    {p.authors?.[0]?.name} et al. • {p.year}
                  </Typography>
                </Box>
                <IconButton size="small" onClick={() => onNavigate(p.paperId)}>
                  <OpenInNew fontSize="inherit" />
                </IconButton>
                <IconButton size="small" onClick={() => setExpandedItem(isItemExpanded ? null : `${p.paperId}-${i}`)}>
                  {isItemExpanded ? <ExpandLess fontSize="inherit" /> : <ExpandMore fontSize="inherit" />}
                </IconButton>
              </Box>

              <Collapse in={isItemExpanded} timeout="auto" unmountOnExit>
                <Box sx={{ p: 2, pb: 2, bgcolor: 'background.paper', borderRadius: 1, mb: 1, border: '1px solid #ddd' }}>
                  {p.abstract ? (
                    <Typography variant="body2" sx={{ fontStyle: 'italic', mb: 1 }}>
                      {p.abstract}
                    </Typography>
                  ) : (
                    <Typography variant="caption" color="text.secondary">No abstract available.</Typography>
                  )}
                  <Box sx={{ display: 'flex', gap: 1, mt: 1, flexWrap: 'wrap' }}>
                    {p.venue && <Box sx={{ fontSize: '10px', px: 1, bgcolor: 'grey.200', borderRadius: 1 }}>{p.venue}</Box>}
                    {p.externalIds?.DOI && <Box sx={{ fontSize: '10px', px: 1, bgcolor: 'blue.50', color: 'primary.main', borderRadius: 1 }}>DOI: {p.externalIds.DOI}</Box>}
                  </Box>
                </Box>
              </Collapse>
            </ListItem>
          </React.Fragment>
        );
      })}
      <Box ref={observerTarget} sx={{ py: 2, textAlign: 'center' }}>
        {loading && <CircularProgress size={24} />}
      </Box>
    </List>
  );
};

export default InfiniteList;

// const InfiniteList: React.FC<{
//   type: 'citations' | 'references',
//   ssid: string,
//   onNavigate: (id: string) => void,
//   initialItems: any[]
// }> = ({ type, ssid, onNavigate, initialItems }) => {

//   // Start with the 100 items already fetched by s2_details
//   const [items, setItems] = useState<any[]>(initialItems);
//   // Set offset to initial length (100) so the next fetch starts at 101
//   const [offset, setOffset] = useState(initialItems.length);
//   const [hasMore, setHasMore] = useState(initialItems.length > 0);
//   const [loading, setLoading] = useState(false);
//   const observerTarget = useRef(null);

//   const fetchData = useCallback(async () => {
//     if (loading || !hasMore) return;
//     setLoading(true);
//     try {
//       // Fetching next batch of 100
//       const response = await fetch(`${BASE_URL}/s2_${type}/${ssid}?offset=${offset}&count=100`);
//       const result = await response.json();

//       if (result.data && result.data.length > 0) {
//         setItems(prev => [...prev, ...result.data]);
//         setOffset(prev => prev + result.data.length);
//         // If 'next' is 0 or null in your Python response, there are no more records
//         setHasMore(result.next !== null && result.next !== undefined);
//       } else {
//         setHasMore(false);
//       }
//     } catch (err) {
//       setHasMore(false);
//     } finally {
//       setLoading(false);
//     }
//   }, [offset, ssid, type, loading, hasMore]);

//   useEffect(() => {
//     const observer = new IntersectionObserver(
//       entries => {
//         // Only trigger if we aren't currently loading
//         if (entries[0].isIntersecting && !loading) {
//           fetchData();
//         }
//       },
//       { threshold: 0.1 }
//     );
//     if (observerTarget.current) observer.observe(observerTarget.current);
//     return () => observer.disconnect();
//   }, [fetchData, loading]);

//   return (
//     <List dense>
//       {items.map((item, i) => {
//         // Support both Citation/Reference objects and direct PaperDetails
//         const p = item.citingPaper || item.citedPaper || item;
//         if (!p || !p.title) return null;

//         return (
//           <ListItem
//             key={`${p.paperId}-${i}`}
//             divider
//             secondaryAction={
//               <IconButton edge="end" onClick={() => onNavigate(p.paperId)}>
//                 <OpenInNew fontSize="small" />
//               </IconButton>
//             }
//           >
//             <ListItemText
//               primary={p.title}
//               secondary={`${p.authors?.[0]?.name || 'Unknown'} • ${p.year || 'N/A'}`}
//               primaryTypographyProps={{ variant: 'body2', fontWeight: 500 }}
//             />
//           </ListItem>
//         );
//       })}
//       <Box ref={observerTarget} sx={{ py: 2, textAlign: 'center' }}>
//         {loading && <CircularProgress size={24} />}
//         {!hasMore && items.length > 0 && (
//           <Typography variant="caption" color="text.secondary">End of list</Typography>
//         )}
//       </Box>
//     </List>
//   );
// };
