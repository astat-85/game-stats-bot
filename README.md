# Game Stats Bot

Telegram bot for collecting game statistics.

## Features
- Multiple game accounts per user
- Step-by-step data entry
- CSV export for admins
- Database backups

## Deployment on Bothost
1. Push this code to GitHub
2. Create new bot on Bothost
3. Connect GitHub repository
4. Add environment variables:
   - `BOT_TOKEN`
   - `ADMIN_IDS`
   - `TARGET_CHAT_ID`
   - `TARGET_TOPIC_ID` (optional)
5. Deploy