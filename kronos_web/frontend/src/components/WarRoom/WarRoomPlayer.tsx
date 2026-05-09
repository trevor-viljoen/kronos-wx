import { useEffect, useRef, useState } from 'react';
import Hls from 'hls.js';

interface Props {
  station: string;
  channelId: string;
  isFocused: boolean;
  onFocus: () => void;
  tabloHost: string;
}

export function WarRoomPlayer({ station, channelId, isFocused, onFocus, tabloHost }: Props) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const hlsRef = useRef<Hls | null>(null);
  const [streamUrl, setStreamUrl] = useState<string | null>(null);
  const [sessionId, setSessionId] = useState<string | null>(null);

  useEffect(() => {
    if (!channelId || !tabloHost) return;

    // 1. Tell tablo-web to start the stream
    const host = tabloHost.replace(/\/$/, '');
    let cancelled = false;
    
    fetch(`${host}/api/stream/${channelId}`, { method: 'POST' })
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (cancelled) {
          // If unmounted before request finished, stop the stream immediately
          if (data?.session_id) fetch(`${host}/api/stream/${data.session_id}`, { method: 'DELETE' }).catch(() => {});
          return;
        }
        
        if (data?.stream_url) {
          setSessionId(data.session_id);
          setStreamUrl(`${host}${data.stream_url}`);
        }
      })
      .catch(console.error);

    return () => {
      cancelled = true;
    };
  }, [channelId, tabloHost]);

  // Stop stream on unmount
  useEffect(() => {
    return () => {
      if (sessionId && tabloHost) {
        const host = tabloHost.replace(/\/$/, '');
        fetch(`${host}/api/stream/${sessionId}`, { method: 'DELETE' }).catch(() => {});
      }
    };
  }, [sessionId, tabloHost]);

  useEffect(() => {
    if (!videoRef.current || !streamUrl) return;

    if (hlsRef.current) {
      hlsRef.current.destroy();
    }

    const hls = new Hls({
      enableWorker: true,
      lowLatencyMode: true,
    });
    
    hls.loadSource(streamUrl);
    hls.attachMedia(videoRef.current);
    hlsRef.current = hls;

    return () => {
      hls.destroy();
      hlsRef.current = null;
    };
  }, [streamUrl]);

  // Sync volume with focus
  useEffect(() => {
    if (videoRef.current) {
      videoRef.current.muted = !isFocused;
      videoRef.current.volume = isFocused ? 1.0 : 0.0;
    }
  }, [isFocused]);

  return (
    <div 
      onClick={onFocus}
      className={`relative group cursor-pointer border transition-all duration-500 rounded-lg overflow-hidden bg-black
                 ${isFocused 
                   ? 'border-accent shadow-[0_0_20px_rgba(var(--accent-rgb),0.4)] scale-[1.02] z-10' 
                   : 'border-white/5 opacity-50 hover:opacity-100'}`}
    >
      <video 
        ref={videoRef} 
        autoPlay 
        playsInline 
        className="w-full h-full aspect-video object-cover" 
      />
      
      {/* Overlay Information */}
      <div className="absolute top-0 inset-x-0 p-3 bg-gradient-to-b from-black/80 to-transparent pointer-events-none">
        <div className="flex items-center justify-between">
          <span className="text-[10px] font-black tracking-[0.2em] uppercase text-white/90">
            {station}
          </span>
          {isFocused && (
            <div className="flex items-center gap-1.5">
              <span className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse" />
              <span className="text-[9px] font-bold text-accent tracking-widest uppercase">Live Audio</span>
            </div>
          )}
        </div>
      </div>

      {/* Hover focus indicator */}
      {!isFocused && (
        <div className="absolute inset-0 bg-accent/5 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center">
           <span className="px-3 py-1 bg-black/60 border border-white/10 rounded-full text-[9px] font-bold tracking-widest uppercase text-white/60">
             Switch Focus
           </span>
        </div>
      )}
    </div>
  );
}
