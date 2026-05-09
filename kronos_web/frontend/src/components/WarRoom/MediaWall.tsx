import { useState, useEffect } from 'react';
import { WarRoomPlayer } from './WarRoomPlayer';

interface Station {
  call_sign: string;
  channel_id: string;
}

interface Props {
  tabloHost: string;
  stations: Station[];
}

export function MediaWall({ tabloHost, stations }: Props) {
  const [focusedIndex, setFocusedIndex] = useState(0);

  // Keyboard listeners for hot-swapping focus (1, 2, 3)
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === '1') setFocusedIndex(0);
      if (e.key === '2' && stations.length > 1) setFocusedIndex(1);
      if (e.key === '3' && stations.length > 2) setFocusedIndex(2);
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [stations.length]);

  if (!stations || stations.length === 0) return null;

  return (
    <div className="flex flex-col gap-3 h-full">
      {/* Hero Slot (Focused Station) */}
      <div className="flex-1 min-h-0">
        <WarRoomPlayer
          station={stations[focusedIndex].call_sign}
          channelId={stations[focusedIndex].channel_id}
          isFocused={true}
          onFocus={() => {}}
          tabloHost={tabloHost}
        />
      </div>

      {/* Scout Slots (Muted Stations) */}
      <div className="grid grid-cols-2 gap-3 h-32 flex-shrink-0">
        {stations.map((s, i) => {
          if (i === focusedIndex) return null;
          return (
            <WarRoomPlayer
              key={s.call_sign}
              station={s.call_sign}
              channelId={s.channel_id}
              isFocused={false}
              onFocus={() => setFocusedIndex(i)}
              tabloHost={tabloHost}
            />
          );
        }).filter(Boolean)}
      </div>
    </div>
  );
}
