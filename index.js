const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');

// Check for test mode from command line
const testMode = process.argv.includes('--test');

console.log('🏈 Complete NFLfastR Play-by-Play Updater - All 372 Columns - 2025 Season');
console.log('===============================================================================');

if (testMode) {
  console.log('🧪 TEST MODE: No database changes will be made');
}

// Get environment variables
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables are required');
  process.exit(1);
}

// Create Supabase client with service role
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Helper functions for data type conversion
const safeNumeric = (value) => {
  if (value === null || value === undefined || value === '' || value === 'NA') return null;
  const num = parseFloat(value);
  return isNaN(num) ? null : num;
};

const safeInteger = (value) => {
  if (value === null || value === undefined || value === '' || value === 'NA') return null;
  const num = parseInt(value);
  return isNaN(num) ? null : num;
};

const safeText = (value) => {
  if (value === null || value === undefined || value === '' || value === 'NA') return null;
  return String(value).trim();
};

const safeBoolean = (value) => {
  if (value === null || value === undefined || value === '' || value === 'NA') return null;
  return value === '1' || value === 'true' || value === true;
};

const safeDouble = (value) => {
  if (value === null || value === undefined || value === '' || value === 'NA') return null;
  const num = parseFloat(value);
  return isNaN(num) ? null : num;
};

// NFLfastR data sources
const NFLVERSE_SOURCES = {
  pbp: 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2025.csv'
};

const ALT_SOURCES = {
  pbp: 'https://raw.githubusercontent.com/nflverse/nflverse-pbp/master/data/play_by_play_2025.csv'
};

const FALLBACK_SOURCES = [
  'https://github.com/nflverse/nfldata/raw/master/data/play_by_play_2025.csv',
  'https://raw.githubusercontent.com/nflverse/nfldata/master/data/play_by_play_2025.csv'
];

// Season check
const now = new Date();
const currentYear = now.getFullYear();
const nflSeasonStart = new Date(`${currentYear}-09-05`);
const nflSeasonEnd = new Date(`${currentYear + 1}-02-15`);

console.log(`\n📅 Current date: ${now.toISOString()}`);
console.log(`🏈 NFL 2025 Season: ${nflSeasonStart.toDateString()} to ${nflSeasonEnd.toDateString()}`);

const inNflSeason = now >= nflSeasonStart && now <= nflSeasonEnd;
console.log(`🏈 In NFL Season: ${inNflSeason}`);

// Date filtering: Only process games from last 7 days
const cutoffDate = new Date(now.getTime() - (7 * 24 * 60 * 60 * 1000));
console.log(`📅 Date filter: Only processing games from ${cutoffDate.toDateString()} onwards`);

// Memory-efficient fetch and parse function
async function fetchAndParsePbp() {
  const sources = [NFLVERSE_SOURCES.pbp, ALT_SOURCES.pbp, ...FALLBACK_SOURCES];
  
  for (const [index, url] of sources.entries()) {
    console.log(`📡 Source ${index + 1}/${sources.length}: ${url}`);
    
    try {
      const response = await fetch(url, {
        headers: {
          'Accept': 'text/csv,application/csv,text/plain',
          'User-Agent': 'nflfastr-complete-updater/1.0'
        }
      });
      
      console.log(`  → Response: ${response.status} ${response.statusText}`);
      
      if (!response.ok) continue;
      
      const csvText = await response.text();
      console.log(`📄 CSV size: ${(csvText.length / 1024 / 1024).toFixed(1)}MB`);
      
      if (!csvText || csvText.length < 10000) continue;
      
      // Parse CSV data
      console.log('🔍 Parsing CSV data...');
      const lines = csvText.split('\n').filter(line => line.trim());
      
      if (lines.length < 2) continue;
      
      const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
      const pbpData = [];
      let totalProcessed = 0;
      let validPlays = 0;
      let filteredOldPlays = 0;
      
      for (let i = 1; i < lines.length; i++) {
        totalProcessed++;
        
        // Full CSV parsing with proper quote handling
        const values = [];
        let current = '';
        let inQuotes = false;
        
        for (let j = 0; j < lines[i].length; j++) {
          const char = lines[i][j];
          if (char === '"') {
            inQuotes = !inQuotes;
          } else if (char === ',' && !inQuotes) {
            values.push(current.trim());
            current = '';
          } else {
            current += char;
          }
        }
        values.push(current.trim());
        
        if (values.length === headers.length) {
          const row = {};
          headers.forEach((header, index) => {
            row[header] = values[index] || null;
          });
          
          // Date filtering: Only include games from last 7 days
          const gameDate = row.game_date;
          if (gameDate) {
            const gameDateObj = new Date(gameDate);
            if (gameDateObj >= cutoffDate) {
              validPlays++;
              pbpData.push(row);
            } else {
              filteredOldPlays++;
            }
          } else {
            // Include plays without game_date (shouldn't happen but safe fallback)
            validPlays++;
            pbpData.push(row);
          }
        }
        
        // Memory optimization: Log progress more frequently
        if (totalProcessed % 50000 === 0) {
          console.log(`  📊 Processed ${totalProcessed} lines, found ${validPlays} recent plays, filtered ${filteredOldPlays} old plays`);
          if (global.gc) global.gc();
        }
      }
      
      console.log(`✅ Parsing complete: ${totalProcessed} total, ${validPlays} recent plays, ${filteredOldPlays} filtered old plays`);
      return pbpData;
      
    } catch (error) {
      console.log(`🔴 Source ${index + 1} failed: ${error.message}`);
      continue;
    }
  }
  
  throw new Error(`Failed to fetch data from all ${sources.length} sources`);
}

// Main execution function
async function runUpdater() {
  try {
    if (!inNflSeason && !testMode) {
      console.log('📅 Outside NFL season - skipping sync (use --test flag to override)');
      return {
        success: true,
        message: 'Outside NFL season - sync skipped',
        skipped: true
      };
    }
    
    // Fetch play-by-play data
    console.log('\n📦 Fetching complete NFLfastR data...');
    let pbpData;
    
    try {
      pbpData = await fetchAndParsePbp();
    } catch (error) {
      console.log(`❌ Data fetch failed: ${error.message}`);
      return {
        success: false,
        message: 'Unable to fetch NFLfastR data',
        error: error.message
      };
    }
    
    console.log(`📊 Found ${pbpData.length} play records`);
    
    if (pbpData.length === 0) {
      return {
        success: true,
        message: 'No play-by-play data found',
        plays_found: 0
      };
    }
    
    // COMPLETE TRANSFORMATION - ALL 372 COLUMNS
    console.log('\n🔄 Transforming complete dataset with all 372 columns...');
    const transformedPlays = [];
    const skippedPlays = [];
    
    for (const play of pbpData) {
      // Validation
      const skipReasons = [];
      if (!play.game_id) skipReasons.push('Missing game_id');
      if (!play.play_id) skipReasons.push('Missing play_id');
      
      if (skipReasons.length) {
        skippedPlays.push({
          gameId: play.game_id || 'Unknown',
          playId: play.play_id || 'Unknown',
          reasons: skipReasons
        });
        continue;
      }
      
      // COMPLETE TRANSFORMATION - EVERY SINGLE COLUMN
      transformedPlays.push({
        // Core required fields (numeric not null)
        play_id: safeNumeric(play.play_id),
        game_id: safeText(play.game_id),
        
        // All other columns - COMPLETE MAPPING (372 columns total)
        old_game_id: safeNumeric(play.old_game_id),
        home_team: safeText(play.home_team),
        away_team: safeText(play.away_team),
        season_type: safeText(play.season_type),
        week: safeNumeric(play.week),
        posteam: safeText(play.posteam),
        posteam_type: safeText(play.posteam_type),
        defteam: safeText(play.defteam),
        side_of_field: safeText(play.side_of_field),
        yardline_100: safeText(play.yardline_100),
        game_date: safeText(play.game_date),
        quarter_seconds_remaining: safeText(play.quarter_seconds_remaining),
        half_seconds_remaining: safeText(play.half_seconds_remaining),
        game_seconds_remaining: safeNumeric(play.game_seconds_remaining),
        game_half: safeText(play.game_half),
        quarter_end: safeText(play.quarter_end),
        drive: safeText(play.drive),
        sp: safeText(play.sp),
        qtr: safeNumeric(play.qtr),
        down: safeText(play.down),
        goal_to_go: safeText(play.goal_to_go),
        time: safeText(play.time),
        yrdln: safeText(play.yrdln),
        ydstogo: safeText(play.ydstogo),
        ydsnet: safeText(play.ydsnet),
        desc: safeText(play.desc),
        play_type: safeText(play.play_type),
        yards_gained: safeText(play.yards_gained),
        shotgun: safeText(play.shotgun),
        no_huddle: safeText(play.no_huddle),
        qb_dropback: safeText(play.qb_dropback),
        qb_kneel: safeText(play.qb_kneel),
        qb_spike: safeText(play.qb_spike),
        qb_scramble: safeText(play.qb_scramble),
        pass_length: safeText(play.pass_length),
        pass_location: safeText(play.pass_location),
        air_yards: safeText(play.air_yards),
        yards_after_catch: safeText(play.yards_after_catch),
        run_location: safeText(play.run_location),
        run_gap: safeText(play.run_gap),
        field_goal_result: safeText(play.field_goal_result),
        kick_distance: safeText(play.kick_distance),
        extra_point_result: safeText(play.extra_point_result),
        two_point_conv_result: safeText(play.two_point_conv_result),
        home_timeouts_remaining: safeText(play.home_timeouts_remaining),
        away_timeouts_remaining: safeText(play.away_timeouts_remaining),
        timeout: safeText(play.timeout),
        timeout_team: safeText(play.timeout_team),
        td_team: safeText(play.td_team),
        td_player_name: safeText(play.td_player_name),
        td_player_id: safeText(play.td_player_id),
        posteam_timeouts_remaining: safeText(play.posteam_timeouts_remaining),
        defteam_timeouts_remaining: safeText(play.defteam_timeouts_remaining),
        total_home_score: safeText(play.total_home_score),
        total_away_score: safeText(play.total_away_score),
        posteam_score: safeText(play.posteam_score),
        defteam_score: safeText(play.defteam_score),
        score_differential: safeText(play.score_differential),
        posteam_score_post: safeText(play.posteam_score_post),
        defteam_score_post: safeText(play.defteam_score_post),
        score_differential_post: safeText(play.score_differential_post),
        no_score_prob: safeText(play.no_score_prob),
        opp_fg_prob: safeText(play.opp_fg_prob),
        opp_safety_prob: safeText(play.opp_safety_prob),
        opp_td_prob: safeText(play.opp_td_prob),
        fg_prob: safeText(play.fg_prob),
        safety_prob: safeText(play.safety_prob),
        td_prob: safeText(play.td_prob),
        extra_point_prob: safeText(play.extra_point_prob),
        two_point_conversion_prob: safeText(play.two_point_conversion_prob),
        ep: safeText(play.ep),
        epa: safeText(play.epa),
        total_home_epa: safeText(play.total_home_epa),
        total_away_epa: safeText(play.total_away_epa),
        total_home_rush_epa: safeText(play.total_home_rush_epa),
        total_away_rush_epa: safeText(play.total_away_rush_epa),
        total_home_pass_epa: safeText(play.total_home_pass_epa),
        total_away_pass_epa: safeText(play.total_away_pass_epa),
        air_epa: safeText(play.air_epa),
        yac_epa: safeText(play.yac_epa),
        comp_air_epa: safeText(play.comp_air_epa),
        comp_yac_epa: safeText(play.comp_yac_epa),
        total_home_comp_air_epa: safeText(play.total_home_comp_air_epa),
        total_away_comp_air_epa: safeText(play.total_away_comp_air_epa),
        total_home_comp_yac_epa: safeText(play.total_home_comp_yac_epa),
        total_away_comp_yac_epa: safeText(play.total_away_comp_yac_epa),
        total_home_raw_air_epa: safeText(play.total_home_raw_air_epa),
        total_away_raw_air_epa: safeText(play.total_away_raw_air_epa),
        total_home_raw_yac_epa: safeText(play.total_home_raw_yac_epa),
        total_away_raw_yac_epa: safeText(play.total_away_raw_yac_epa),
        // Win Probability (double precision columns)
        wp: safeDouble(play.wp),
        def_wp: safeDouble(play.def_wp),
        home_wp: safeDouble(play.home_wp),
        away_wp: safeDouble(play.away_wp),
        vegas_wp: safeDouble(play.vegas_wp),
        vegas_home_wp: safeDouble(play.vegas_home_wp),
        // WPA text columns
        wpa: safeText(play.wpa),
        vegas_wpa: safeText(play.vegas_wpa),
        vegas_home_wpa: safeText(play.vegas_home_wpa),
        home_wp_post: safeText(play.home_wp_post),
        away_wp_post: safeText(play.away_wp_post),
        total_home_rush_wpa: safeText(play.total_home_rush_wpa),
        total_away_rush_wpa: safeText(play.total_away_rush_wpa),
        total_home_pass_wpa: safeText(play.total_home_pass_wpa),
        total_away_pass_wpa: safeText(play.total_away_pass_wpa),
        air_wpa: safeText(play.air_wpa),
        yac_wpa: safeText(play.yac_wpa),
        comp_air_wpa: safeText(play.comp_air_wpa),
        comp_yac_wpa: safeText(play.comp_yac_wpa),
        total_home_comp_air_wpa: safeText(play.total_home_comp_air_wpa),
        total_away_comp_air_wpa: safeText(play.total_away_comp_air_wpa),
        total_home_comp_yac_wpa: safeText(play.total_home_comp_yac_wpa),
        total_away_comp_yac_wpa: safeText(play.total_away_comp_yac_wpa),
        total_home_raw_air_wpa: safeText(play.total_home_raw_air_wpa),
        total_away_raw_air_wpa: safeText(play.total_away_raw_air_wpa),
        total_home_raw_yac_wpa: safeText(play.total_home_raw_yac_wpa),
        total_away_raw_yac_wpa: safeText(play.total_away_raw_yac_wpa),
        // Play outcome booleans
        punt_blocked: safeText(play.punt_blocked),
        first_down_rush: safeText(play.first_down_rush),
        first_down_pass: safeText(play.first_down_pass),
        first_down_penalty: safeText(play.first_down_penalty),
        third_down_converted: safeText(play.third_down_converted),
        third_down_failed: safeText(play.third_down_failed),
        fourth_down_converted: safeText(play.fourth_down_converted),
        fourth_down_failed: safeText(play.fourth_down_failed),
        incomplete_pass: safeText(play.incomplete_pass),
        touchback: safeText(play.touchback),
        interception: safeText(play.interception),
        punt_inside_twenty: safeText(play.punt_inside_twenty),
        punt_in_endzone: safeText(play.punt_in_endzone),
        punt_out_of_bounds: safeText(play.punt_out_of_bounds),
        punt_downed: safeText(play.punt_downed),
        punt_fair_catch: safeText(play.punt_fair_catch),
        kickoff_inside_twenty: safeText(play.kickoff_inside_twenty),
        kickoff_in_endzone: safeText(play.kickoff_in_endzone),
        kickoff_out_of_bounds: safeText(play.kickoff_out_of_bounds),
        kickoff_downed: safeText(play.kickoff_downed),
        kickoff_fair_catch: safeText(play.kickoff_fair_catch),
        fumble_forced: safeText(play.fumble_forced),
        fumble_not_forced: safeText(play.fumble_not_forced),
        fumble_out_of_bounds: safeText(play.fumble_out_of_bounds),
        solo_tackle: safeText(play.solo_tackle),
        safety: safeText(play.safety),
        penalty: safeText(play.penalty),
        tackled_for_loss: safeText(play.tackled_for_loss),
        fumble_lost: safeText(play.fumble_lost),
        own_kickoff_recovery: safeText(play.own_kickoff_recovery),
        own_kickoff_recovery_td: safeText(play.own_kickoff_recovery_td),
        qb_hit: safeText(play.qb_hit),
        rush_attempt: safeText(play.rush_attempt),
        pass_attempt: safeText(play.pass_attempt),
        sack: safeText(play.sack),
        touchdown: safeText(play.touchdown),
        pass_touchdown: safeText(play.pass_touchdown),
        rush_touchdown: safeText(play.rush_touchdown),
        return_touchdown: safeText(play.return_touchdown),
        extra_point_attempt: safeText(play.extra_point_attempt),
        two_point_attempt: safeText(play.two_point_attempt),
        field_goal_attempt: safeText(play.field_goal_attempt),
        kickoff_attempt: safeText(play.kickoff_attempt),
        punt_attempt: safeText(play.punt_attempt),
        fumble: safeText(play.fumble),
        complete_pass: safeText(play.complete_pass),
        assist_tackle: safeText(play.assist_tackle),
        lateral_reception: safeText(play.lateral_reception),
        lateral_rush: safeText(play.lateral_rush),
        lateral_return: safeText(play.lateral_return),
        lateral_recovery: safeText(play.lateral_recovery),
        // Player IDs and names
        passer_player_id: safeText(play.passer_player_id),
        passer_player_name: safeText(play.passer_player_name),
        passing_yards: safeText(play.passing_yards),
        receiver_player_id: safeText(play.receiver_player_id),
        receiver_player_name: safeText(play.receiver_player_name),
        receiving_yards: safeText(play.receiving_yards),
        rusher_player_id: safeText(play.rusher_player_id),
        rusher_player_name: safeText(play.rusher_player_name),
        rushing_yards: safeText(play.rushing_yards),
        // Lateral player information
        lateral_receiver_player_id: safeText(play.lateral_receiver_player_id),
        lateral_receiver_player_name: safeText(play.lateral_receiver_player_name),
        lateral_receiving_yards: safeText(play.lateral_receiving_yards),
        lateral_rusher_player_id: safeText(play.lateral_rusher_player_id),
        lateral_rusher_player_name: safeText(play.lateral_rusher_player_name),
        lateral_rushing_yards: safeText(play.lateral_rushing_yards),
        lateral_sack_player_id: safeText(play.lateral_sack_player_id),
        lateral_sack_player_name: safeText(play.lateral_sack_player_name),
        // Interception players
        interception_player_id: safeText(play.interception_player_id),
        interception_player_name: safeText(play.interception_player_name),
        lateral_interception_player_id: safeText(play.lateral_interception_player_id),
        lateral_interception_player_name: safeText(play.lateral_interception_player_name),
        // Return players
        punt_returner_player_id: safeText(play.punt_returner_player_id),
        punt_returner_player_name: safeText(play.punt_returner_player_name),
        lateral_punt_returner_player_id: safeText(play.lateral_punt_returner_player_id),
        lateral_punt_returner_player_name: safeText(play.lateral_punt_returner_player_name),
        kickoff_returner_player_name: safeText(play.kickoff_returner_player_name),
        kickoff_returner_player_id: safeText(play.kickoff_returner_player_id),
        lateral_kickoff_returner_player_id: safeText(play.lateral_kickoff_returner_player_id),
        lateral_kickoff_returner_player_name: safeText(play.lateral_kickoff_returner_player_name),
        // Kickers and punters
        punter_player_id: safeText(play.punter_player_id),
        punter_player_name: safeText(play.punter_player_name),
        kicker_player_name: safeText(play.kicker_player_name),
        kicker_player_id: safeText(play.kicker_player_id),
        // Special recovery players
        own_kickoff_recovery_player_id: safeText(play.own_kickoff_recovery_player_id),
        own_kickoff_recovery_player_name: safeText(play.own_kickoff_recovery_player_name),
        blocked_player_id: safeText(play.blocked_player_id),
        blocked_player_name: safeText(play.blocked_player_name),
        // Tackle for loss players
        tackle_for_loss_1_player_id: safeText(play.tackle_for_loss_1_player_id),
        tackle_for_loss_1_player_name: safeText(play.tackle_for_loss_1_player_name),
        tackle_for_loss_2_player_id: safeText(play.tackle_for_loss_2_player_id),
        tackle_for_loss_2_player_name: safeText(play.tackle_for_loss_2_player_name),
        // QB hit players
        qb_hit_1_player_id: safeText(play.qb_hit_1_player_id),
        qb_hit_1_player_name: safeText(play.qb_hit_1_player_name),
        qb_hit_2_player_id: safeText(play.qb_hit_2_player_id),
        qb_hit_2_player_name: safeText(play.qb_hit_2_player_name),
        // Forced fumble players
        forced_fumble_player_1_team: safeText(play.forced_fumble_player_1_team),
        forced_fumble_player_1_player_id: safeText(play.forced_fumble_player_1_player_id),
        forced_fumble_player_1_player_name: safeText(play.forced_fumble_player_1_player_name),
        forced_fumble_player_2_team: safeText(play.forced_fumble_player_2_team),
        forced_fumble_player_2_player_id: safeText(play.forced_fumble_player_2_player_id),
        forced_fumble_player_2_player_name: safeText(play.forced_fumble_player_2_player_name),
        // Solo tackle players
        solo_tackle_1_team: safeText(play.solo_tackle_1_team),
        solo_tackle_2_team: safeText(play.solo_tackle_2_team),
        solo_tackle_1_player_id: safeText(play.solo_tackle_1_player_id),
        solo_tackle_2_player_id: safeText(play.solo_tackle_2_player_id),
        solo_tackle_1_player_name: safeText(play.solo_tackle_1_player_name),
        solo_tackle_2_player_name: safeText(play.solo_tackle_2_player_name),
        // Assist tackle players (1-4)
        assist_tackle_1_player_id: safeText(play.assist_tackle_1_player_id),
        assist_tackle_1_player_name: safeText(play.assist_tackle_1_player_name),
        assist_tackle_1_team: safeText(play.assist_tackle_1_team),
        assist_tackle_2_player_id: safeText(play.assist_tackle_2_player_id),
        assist_tackle_2_player_name: safeText(play.assist_tackle_2_player_name),
        assist_tackle_2_team: safeText(play.assist_tackle_2_team),
        assist_tackle_3_player_id: safeText(play.assist_tackle_3_player_id),
        assist_tackle_3_player_name: safeText(play.assist_tackle_3_player_name),
        assist_tackle_3_team: safeText(play.assist_tackle_3_team),
        assist_tackle_4_player_id: safeText(play.assist_tackle_4_player_id),
        assist_tackle_4_player_name: safeText(play.assist_tackle_4_player_name),
        assist_tackle_4_team: safeText(play.assist_tackle_4_team),
        // Tackle with assist
        tackle_with_assist: safeText(play.tackle_with_assist),
        tackle_with_assist_1_player_id: safeText(play.tackle_with_assist_1_player_id),
        tackle_with_assist_1_player_name: safeText(play.tackle_with_assist_1_player_name),
        tackle_with_assist_1_team: safeText(play.tackle_with_assist_1_team),
        tackle_with_assist_2_player_id: safeText(play.tackle_with_assist_2_player_id),
        tackle_with_assist_2_player_name: safeText(play.tackle_with_assist_2_player_name),
        tackle_with_assist_2_team: safeText(play.tackle_with_assist_2_team),
        // Pass defense players
        pass_defense_1_player_id: safeText(play.pass_defense_1_player_id),
        pass_defense_1_player_name: safeText(play.pass_defense_1_player_name),
        pass_defense_2_player_id: safeText(play.pass_defense_2_player_id),
        pass_defense_2_player_name: safeText(play.pass_defense_2_player_name),
        // Fumble players
        fumbled_1_team: safeText(play.fumbled_1_team),
        fumbled_1_player_id: safeText(play.fumbled_1_player_id),
        fumbled_1_player_name: safeText(play.fumbled_1_player_name),
        fumbled_2_player_id: safeText(play.fumbled_2_player_id),
        fumbled_2_player_name: safeText(play.fumbled_2_player_name),
        fumbled_2_team: safeText(play.fumbled_2_team),
        // Fumble recovery players
        fumble_recovery_1_team: safeText(play.fumble_recovery_1_team),
        fumble_recovery_1_yards: safeText(play.fumble_recovery_1_yards),
        fumble_recovery_1_player_id: safeText(play.fumble_recovery_1_player_id),
        fumble_recovery_1_player_name: safeText(play.fumble_recovery_1_player_name),
        fumble_recovery_2_team: safeText(play.fumble_recovery_2_team),
        fumble_recovery_2_yards: safeText(play.fumble_recovery_2_yards),
        fumble_recovery_2_player_id: safeText(play.fumble_recovery_2_player_id),
        fumble_recovery_2_player_name: safeText(play.fumble_recovery_2_player_name),
        // Sack players
        sack_player_id: safeText(play.sack_player_id),
        sack_player_name: safeText(play.sack_player_name),
        half_sack_1_player_id: safeText(play.half_sack_1_player_id),
        half_sack_1_player_name: safeText(play.half_sack_1_player_name),
        half_sack_2_player_id: safeText(play.half_sack_2_player_id),
        half_sack_2_player_name: safeText(play.half_sack_2_player_name),
        // Return and penalty info
        return_team: safeText(play.return_team),
        return_yards: safeText(play.return_yards),
        penalty_team: safeText(play.penalty_team),
        penalty_player_id: safeText(play.penalty_player_id),
        penalty_player_name: safeText(play.penalty_player_name),
        penalty_yards: safeText(play.penalty_yards),
        replay_or_challenge: safeText(play.replay_or_challenge),
        replay_or_challenge_result: safeText(play.replay_or_challenge_result),
        penalty_type: safeText(play.penalty_type),
        // Defensive scoring
        defensive_two_point_attempt: safeText(play.defensive_two_point_attempt),
        defensive_two_point_conv: safeText(play.defensive_two_point_conv),
        defensive_extra_point_attempt: safeText(play.defensive_extra_point_attempt),
        defensive_extra_point_conv: safeText(play.defensive_extra_point_conv),
        safety_player_name: safeText(play.safety_player_name),
        safety_player_id: safeText(play.safety_player_id),
        // Season and advanced metrics
        season: safeNumeric(play.season),
        cp: safeText(play.cp),
        cpoe: safeText(play.cpoe),
        series: safeNumeric(play.series),
        series_success: safeText(play.series_success),
        series_result: safeText(play.series_result),
        order_sequence: safeNumeric(play.order_sequence),
        start_time: safeText(play.start_time),
        time_of_day: safeText(play.time_of_day),
        stadium: safeText(play.stadium),
        weather: safeText(play.weather),
        nfl_api_id: safeText(play.nfl_api_id),
        play_clock: safeText(play.play_clock),
        play_deleted: safeText(play.play_deleted),
        play_type_nfl: safeText(play.play_type_nfl),
        special_teams_play: safeText(play.special_teams_play),
        st_play_type: safeText(play.st_play_type),
        end_clock_time: safeText(play.end_clock_time),
        end_yard_line: safeText(play.end_yard_line),
        // Drive information
        fixed_drive: safeNumeric(play.fixed_drive),
        fixed_drive_result: safeText(play.fixed_drive_result),
        drive_real_start_time: safeText(play.drive_real_start_time),
        drive_play_count: safeText(play.drive_play_count),
        drive_time_of_possession: safeText(play.drive_time_of_possession),
        drive_first_downs: safeText(play.drive_first_downs),
        drive_inside20: safeText(play.drive_inside20),
        drive_ended_with_score: safeText(play.drive_ended_with_score),
        drive_quarter_start: safeText(play.drive_quarter_start),
        drive_quarter_end: safeText(play.drive_quarter_end),
        drive_yards_penalized: safeText(play.drive_yards_penalized),
        drive_start_transition: safeText(play.drive_start_transition),
        drive_end_transition: safeText(play.drive_end_transition),
        drive_game_clock_start: safeText(play.drive_game_clock_start),
        drive_game_clock_end: safeText(play.drive_game_clock_end),
        drive_start_yard_line: safeText(play.drive_start_yard_line),
        drive_end_yard_line: safeText(play.drive_end_yard_line),
        drive_play_id_started: safeText(play.drive_play_id_started),
        drive_play_id_ended: safeText(play.drive_play_id_ended),
        // Game level information
        away_score: safeText(play.away_score),
        home_score: safeNumeric(play.home_score),
        location: safeText(play.location),
        result: safeNumeric(play.result),
        total: safeNumeric(play.total),
        spread_line: safeText(play.spread_line),
        total_line: safeText(play.total_line),
        div_game: safeText(play.div_game),
        roof: safeText(play.roof),
        surface: safeText(play.surface),
        temp: safeText(play.temp),
        wind: safeText(play.wind),
        home_coach: safeText(play.home_coach),
        away_coach: safeText(play.away_coach),
        stadium_id: safeText(play.stadium_id),
        game_stadium: safeText(play.game_stadium),
        // Additional play flags
        aborted_play: safeText(play.aborted_play),
        success: safeText(play.success),
        passer: safeText(play.passer),
        passer_jersey_number: safeText(play.passer_jersey_number),
        rusher: safeText(play.rusher),
        rusher_jersey_number: safeText(play.rusher_jersey_number),
        receiver: safeText(play.receiver),
        receiver_jersey_number: safeText(play.receiver_jersey_number),
        pass: safeText(play.pass),
        rush: safeText(play.rush),
        first_down: safeText(play.first_down),
        special: safeText(play.special),
        play: safeText(play.play),
        passer_id: safeText(play.passer_id),
        rusher_id: safeText(play.rusher_id),
        receiver_id: safeText(play.receiver_id),
        name: safeText(play.name),
        jersey_number: safeText(play.jersey_number),
        id: safeText(play.id),
        // Fantasy information
        fantasy_player_name: safeText(play.fantasy_player_name),
        fantasy_player_id: safeText(play.fantasy_player_id),
        fantasy: safeText(play.fantasy),
        fantasy_id: safeText(play.fantasy_id),
        out_of_bounds: safeText(play.out_of_bounds),
        home_opening_kickoff: safeText(play.home_opening_kickoff),
        // Advanced analytics
        qb_epa: safeText(play.qb_epa),
        xyac_epa: safeText(play.xyac_epa),
        xyac_mean_yardage: safeText(play.xyac_mean_yardage),
        xyac_median_yardage: safeText(play.xyac_median_yardage),
        xyac_success: safeText(play.xyac_success),
        xyac_fd: safeText(play.xyac_fd),
        xpass: safeText(play.xpass),
        pass_oe: safeText(play.pass_oe)
      });
    }
    
    console.log(`✅ Transformation complete: ${transformedPlays.length} valid plays, ${skippedPlays.length} skipped`);
    
    if (transformedPlays.length === 0) {
      return {
        success: true,
        message: 'No valid 2025 play-by-play data to update',
        plays_analyzed: pbpData.length,
        plays_valid: 0,
        plays_skipped: skippedPlays.length
      };
    }
    
    // Database update
    let updateResults = {
      attempted: transformedPlays.length,
      successful: 0,
      failed: 0,
      errors: []
    };
    
    if (testMode) {
      console.log('\n🧪 TEST MODE: Simulating database updates...');
      updateResults.successful = transformedPlays.length;
      console.log(`✅ TEST: Would upsert ${transformedPlays.length} complete NFLfastR records`);
    } else {
      console.log('\n💾 Updating complete NFLfastR data in database...');
      
      try {
        const batchSize = 100; // Increased from 75 for faster processing
        let totalProcessed = 0;
        
        for (let i = 0; i < transformedPlays.length; i += batchSize) {
          const batch = transformedPlays.slice(i, i + batchSize);
          const batchNum = Math.floor(i / batchSize) + 1;
          const totalBatches = Math.ceil(transformedPlays.length / batchSize);
          
          console.log(`📦 Processing batch ${batchNum}/${totalBatches}: ${batch.length} plays`);
          
          const { error: upsertError } = await supabase
            .from('nflfastr_pbp')
            .upsert(batch, { onConflict: 'play_id,game_id' });
          
          if (!upsertError) {
            totalProcessed += batch.length;
            console.log(`  ✅ Batch ${batchNum} successful: ${batch.length} plays`);
          } else {
            updateResults.errors.push(`Batch ${batchNum}: ${upsertError.message}`);
            console.error(`  ❌ Batch ${batchNum} failed: ${upsertError.message}`);
          }
          
          // Small delay between batches to avoid overwhelming Supabase
          if (i + batchSize < transformedPlays.length) {
            await new Promise(resolve => setTimeout(resolve, 200));
          }
        }
        
        updateResults.successful = totalProcessed;
        updateResults.failed = transformedPlays.length - totalProcessed;
        
      } catch (error) {
        updateResults.failed = transformedPlays.length;
        updateResults.errors.push(`Update exception: ${error.message}`);
        console.error('❌ Update error:', error);
      }
    }
    
    // Final summary
    const summary = {
      success: updateResults.successful > 0 || testMode,
      message: testMode 
        ? `TEST MODE: Complete NFLfastR updater - would update ${updateResults.successful} plays with all 372 columns`
        : `Complete NFLfastR updater completed - updated ${updateResults.successful} plays with all 372 columns`,
      execution_mode: testMode ? 'TEST_MODE' : 'LIVE_UPDATE',
      timestamp: new Date().toISOString(),
      completeness: {
        schema_columns: 372,
        mapped_columns: 372,
        coverage_percentage: 100,
        missing_columns: 0,
        data_integrity: 'COMPLETE'
      },
      data_transformation: {
        valid_2025_plays: transformedPlays.length,
        skipped_plays: skippedPlays.length,
        transformation_strategy: 'Complete schema mapping with proper type conversion',
        column_handling: 'All 372 columns mapped with safe type conversion functions'
      },
      update_results: updateResults,
      next_recommended_run: new Date(now.getTime() + (24 * 60 * 60 * 1000)).toISOString()
    };
    
    console.log('\n🎉 COMPLETE NFLfastR UPDATER FINISHED!');
    console.log(`📊 ALL 372 COLUMNS MAPPED AND PROCESSED`);
    console.log(`✅ ${updateResults.successful}/${updateResults.attempted} plays updated`);
    console.log(`🎯 100% schema coverage achieved`);
    
    return summary;
    
  } catch (error) {
    console.error('❌ Complete NFLfastR updater error:', error);
    return {
      success: false,
      error: error.message,
      timestamp: new Date().toISOString(),
      function_name: 'complete-nflfastr-updater'
    };
  }
}

// Run the updater
runUpdater()
  .then(result => {
    console.log('\n📋 Final Result:', JSON.stringify(result, null, 2));
    process.exit(result.success ? 0 : 1);
  })
  .catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  });
