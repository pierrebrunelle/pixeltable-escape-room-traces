"""Write the sample rooms and guest messages (fictional, offline) to data/."""

import json
from pathlib import Path

DATA = Path(__file__).parent / 'data'

ROOMS = [
    {'room_id': 'vault', 'name': 'The Midnight Vault', 'max_party': 6, 'minutes': 60},
    {'room_id': 'lab', 'name': 'Dr. Quill\'s Laboratory', 'max_party': 5, 'minutes': 75},
    {'room_id': 'lighthouse', 'name': 'Lighthouse Keeper', 'max_party': 8, 'minutes': 90},
]

MESSAGES = [
    ('vault', 4, 'Hi! Can we book the Midnight Vault for four people this Saturday around 7pm?'),
    ('lab', 5, 'We booked the Laboratory for Friday but one person is sick. Can we move to next week?'),
    ('lighthouse', 8, 'Is the Lighthouse room wheelchair accessible? My uncle uses a chair.'),
    ('vault', 3, 'Please cancel booking EV-2291, we cannot make it anymore.'),
    ('lab', 2, 'The clue lock in the lab was broken during our game last night and staff ignored us. Very disappointed.'),
    ('lighthouse', 6, 'Birthday party for six on Sunday afternoon, any slot after 2pm works.'),
    ('vault', 6, 'Do you have gift cards? I want one for my sister.'),
    ('lab', 4, 'Our team-building event is in two hours and we still have no confirmation email!'),
    ('lighthouse', 7, 'Can we bring snacks and a cake into the lobby?'),
    ('vault', 2, 'Can I switch our 6pm Thursday slot to 8pm?'),
    ('lab', 5, 'Corporate group of 5 wants the Laboratory next Wednesday at noon.'),
    ('lighthouse', 4, 'We were charged twice for the same booking. Please refund one.'),
    ('vault', 5, 'Is there parking near the venue?'),
    ('lab', 3, 'Cancel our Sunday booking please, flight got changed.'),
    ('lighthouse', 8, 'Book the Lighthouse Keeper for 8 next Friday at 6pm, we are a scout troop.'),
    ('vault', 4, 'The game master was amazing but the room was really hot, maybe fix the AC?'),
    ('lab', 2, 'Can kids under 12 play the Laboratory?'),
    ('lighthouse', 5, 'Need to move our booking from 3pm to 5pm today, sorry for the short notice!'),
]


def main() -> None:
    DATA.mkdir(exist_ok=True)
    (DATA / 'rooms.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in ROOMS))
    for t in range(3):
        rows = []
        for i, (room, party, msg) in enumerate(MESSAGES[t * 6 : (t + 1) * 6]):
            n = t * 6 + i + 1
            rows.append(
                {
                    'request_id': f'R{n:03d}',
                    'tick': f't{t + 1}',
                    'room': room,
                    'party_size': party,
                    'received_at': f'2026-11-24T{8 + t:02d}:{i * 9:02d}:00-08:00',
                    'message': msg,
                }
            )
        (DATA / f'requests-t{t + 1}.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in rows))
    print('wrote', len(ROOMS), 'rooms and', len(MESSAGES), 'messages to', DATA.name + '/')


if __name__ == '__main__':
    main()
