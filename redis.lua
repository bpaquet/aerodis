local MAX_INT = 4294967294 - 2

local function EXISTS(rec, bin)
	if aerospike:exists(rec)
		and rec[bin] ~= nil
			and type(rec) == "userdata"
				and record.ttl(rec) < (MAX_INT - 60) then
		return true
	end
	return false
end

local function UPDATE(rec)
	if aerospike:exists(rec) then
		aerospike:update(rec)
	else
		aerospike:create(rec)
	end
end

function HSET (rec, bin, value)
	local created = 1
	if (EXISTS(rec, bin)) then
		created = 0
	end
	rec[bin] = value
	UPDATE(rec)
	return created
end

function HDEL (rec, bin)
	if (EXISTS(rec, bin)) then
		rec[bin] = nil
		UPDATE(rec)
		return 1
	end
	return 0
end
