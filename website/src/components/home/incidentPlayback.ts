export const DURATION = 12;
export const EVENTS = [
  {id: 'stall', label: 'Sink stall', time: 5.35,
    description: 'The queue fills to its bound. Source lanes pause while source event polling continues; intake resumes when pressure clears.'},
  {id: 'bad', label: 'Bad record', time: 7.6,
    description: 'With the explicit Skip policy, the failed record is counted and skipped. Choosing Fail instead stops the pipeline.'},
  {id: 'shutdown', label: 'Shutdown', time: 9.65,
    description: 'Intake stops. Pending work drains until completion or the configured deadline; only acknowledged data is committed.'},
] as const;

export function phaseAt(time: number) {
  if (time >= DURATION) return {id: 'done', label: 'Shutdown complete',
    description: 'In this illustration, pending work finishes before the deadline. Acknowledged data is committed and the process exits.'};
  if (time >= 9) return EVENTS[2];
  if (time >= 7) return EVENTS[1];
  if (time >= 3 && time <= 5.8) return EVENTS[0];
  return {id: 'flow', label: time < 3 ? 'Pipeline running' : 'Intake resumes',
    description: 'Records pass through the transform and bounded queue to the sink. Acknowledgements return before the committed watermark advances.'};
}

export type Playback = {time: number; playing: boolean; reduced: boolean; run: number};
export type Action = {type: 'select'; time: number} | {type: 'tick'; time: number} |
  {type: 'motion'; reduced: boolean} | {type: 'toggle'} | {type: 'replay'};
export const INITIAL: Playback = {time: EVENTS[0].time, playing: false, reduced: false, run: 0};

/** Playback stops at the final frame; event selection remains available without motion. */
export function playback(state: Playback, action: Action): Playback {
  switch (action.type) {
    case 'select': return {...state, time: action.time, playing: false};
    case 'motion': return {...state, reduced: action.reduced, playing: action.reduced ? false : state.playing};
    case 'toggle': return state.reduced ? state : {...state,
      time: state.time >= DURATION ? 0 : state.time, playing: !state.playing};
    case 'replay': return state.reduced ? state : {...state, time: 0, playing: true, run: state.run + 1};
    case 'tick': return !state.playing ? state : {...state,
      time: Math.min(action.time, DURATION), playing: action.time < DURATION};
  }
}
