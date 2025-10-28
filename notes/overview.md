# Overview

## Usage Patterns

Default Session

- User opens Following Feed (default view)

Notification-Led Session

- If User is followed by Profile, User clicks on Profile
- If User is replied to by Profile, User clicks on Reply and Profile
- If User is quoted by Profile, User clicks on Quote and Profile
- If User Post is liked by Profile, User clicks on Profile
- If User is mentioned by Profile, User clicks on Post Mention and Profile

## Views

Feed View

1. User opens Feed
2. Display top 20 posts -> store as "seen"
3. Load Feed's next K posts depending on user interactions -> store as "seen"

Profile View

1. User clicks on Profile
2. Display Profile's top 5 posts -> store as "seen"
3. Load Profile's next K posts depending on user interactions -> store as "seen"

Thread View

1. User clicks on Post
2. Display Post's parent thread -> store as "seen"

Search View (unable to recreate)

1. User searches for Term
2. Display top N posts -> store as "seen"
