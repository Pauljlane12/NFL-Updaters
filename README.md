# NFL Play-by-Play Data Updater 🏈

Automated NFL play-by-play data updater for Supabase using nflfastr data. Designed to run on Render with no timeout issues.

## Features

- ✅ **Complete Data Coverage**: All 372 nflfastr columns mapped and processed
- ✅ **No Timeout Issues**: Optimized for long-running processes on Render
- ✅ **Memory Efficient**: Batch processing with garbage collection
- ✅ **Multiple Data Sources**: Falls back through multiple nflverse sources
- ✅ **Smart Filtering**: Only processes recent games (last 7 days)
- ✅ **Test Mode**: Dry-run capability for testing

## Quick Start

### Prerequisites

- Node.js 18+ 
- Supabase project with `nflfastr_pbp` table
- Render account (for deployment)

### Local Setup

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/NFL-Updaters.git
cd NFL-Updaters
```

2. **Install dependencies**
```bash
npm install
```

3. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env with your Supabase credentials
```

4. **Test run**
```bash
npm run test
```

5. **Production run**
```bash
npm start
```

## Deployment to Render

### Option 1: One-Click Deploy

Click the button below to deploy to Render:

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy)

### Option 2: Manual Deploy

1. **Create a new Cron Job on Render**
   - Go to https://dashboard.render.com
   - Click "New +" → "Cron Job"
   - Connect your GitHub repository

2. **Configure the service**
   - **Name**: `nfl-pbp-updater`
   - **Build Command**: `npm install`
   - **Start Command**: `npm start`
   - **Schedule**: `0 */6 * * *` (every 6 hours) or `0 2 * * *` (daily at 2am)

3. **Set environment variables**
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_SERVICE_ROLE_KEY`: Your Supabase service role key

4. **Deploy**

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `SUPABASE_URL` | Your Supabase project URL | Yes |
| `SUPABASE_SERVICE_ROLE_KEY` | Your Supabase service role key | Yes |

## Cron Schedule Examples

```bash
# Every 6 hours
0 */6 * * *

# Daily at 2 AM
0 2 * * *

# Every hour during NFL season
0 * * * *

# Every day at 3 AM and 3 PM
0 3,15 * * *
```

## Data Sources

The updater tries multiple nflverse data sources in order:

1. `nflverse-data` releases (primary)
2. `nflverse-pbp` repository
3. `nfldata` repository (fallback)

## Database Schema

Requires a Supabase table named `nflfastr_pbp` with all 372 nflfastr columns. 

**Unique constraint**: `(play_id, game_id)`

## Logging

All operations are logged to stdout:
- 📡 Data source attempts
- 📊 Parsing progress
- 💾 Database updates
- ✅ Success confirmations
- ❌ Error details

## Performance

- **Batch Size**: 100 plays per batch
- **Rate Limiting**: 200ms between batches
- **Memory**: Optimized with periodic garbage collection
- **Timeout**: No timeout issues on Render (unlike Supabase Edge Functions)

## Test Mode

Run in test mode to see what would be updated without making changes:

```bash
npm run test
# or
node index.js --test
```

## Troubleshooting

### "No data found"
- Check that it's NFL season (Sept-Feb)
- Verify nflverse sources are accessible
- Use `--test` flag to see what data is being fetched

### "Supabase error"
- Verify your `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`
- Check table name is exactly `nflfastr_pbp`
- Ensure unique constraint on `(play_id, game_id)` exists

### "Out of memory"
- Reduce batch size in `index.js` (line with `batchSize = 100`)
- Increase Render instance size if needed

## Contributing

Pull requests welcome! Please ensure:
- Code follows existing style
- Test mode works correctly
- All 372 columns remain mapped

## License

MIT

## Author

Paul - Propify

---

**For Propify NFL Analytics Platform** 🏈📊
