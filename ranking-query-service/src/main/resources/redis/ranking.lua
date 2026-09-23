-- All keys must live on one Redis primary. The script is bounded by partition count, not player count.
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local asOf = now
local population = 0
for i = 1,#KEYS,3 do
	local ready = tonumber(redis.call('GET', KEYS[i+1]))
	if not ready or now-ready > 5000 or ready-now > 1000 or redis.call('EXISTS', KEYS[i+2]) == 0 then
		return redis.error_reply('RANKING_UNAVAILABLE')
	end
	asOf = math.min(asOf, ready)
	population = population + redis.call('ZCARD', KEYS[i])
end
local result = {string.format('%.0f', asOf), tostring(population)}
local candidates = {}
local unique = {}
local function add(player, score)
	if not unique[player] then
		candidates[#candidates+1] = {player = player, score = tonumber(score)}
		unique[player] = true
	end
end
local function collect(key, start, finish)
	local rows = redis.call('ZREVRANGE', key, start, finish, 'WITHSCORES')
	for j = 1,#rows,2 do
		add(rows[j], rows[j+1])
	end
end
local mode = ARGV[1]
local player = ARGV[2]
local globalRank = 0
if mode == 'top' then
	for i = 1,#KEYS,3 do
		collect(KEYS[i], 0, 9)
	end
else
	local score = nil
	for i = 1,#KEYS,3 do
		local found = redis.call('ZSCORE', KEYS[i], player)
		if found then
			score = found
		end
	end
	if not score then
		return result
	end
	for i = 1,#KEYS,3 do
		local inserted = redis.call('ZADD', KEYS[i], 'NX', score, player)
		local rank = redis.call('ZREVRANK', KEYS[i], player)
		globalRank = globalRank + rank
		if mode == 'neighbors' then
			collect(KEYS[i], math.max(0, rank-2), rank+2)
		end
		if inserted == 1 then
			redis.call('ZREM', KEYS[i], player)
		end
	end
	if mode == 'rank' then
		result[#result+1] = player
		result[#result+1] = score
		result[#result+1] = tostring(globalRank+1)
		return result
	end
end
table.sort(candidates, function(a,b)
	if a.score == b.score then
		return a.player > b.player
	end
	return a.score > b.score
end)
local start = 1
local finish = math.min(10, #candidates)
local offset = 0
if mode == 'neighbors' then
	for i,row in ipairs(candidates) do
		if row.player == player then
			start = math.max(1, i-2)
			finish = math.min(#candidates, i+2)
			offset = globalRank+1-i
			break
		end
	end
end
for i = start,finish do
	result[#result+1] = candidates[i].player
	result[#result+1] = string.format('%.0f', candidates[i].score)
	result[#result+1] = tostring(i+offset)
end
return result
