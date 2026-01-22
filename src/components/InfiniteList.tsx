import React, { useState, useEffect, useRef, useCallback, memo } from 'react';
import { Typography, Box, List, CircularProgress } from '@mui/material';

import PaperListItem from "./PaperListItem";

const BASE_URL = 'http://localhost:9999';


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
