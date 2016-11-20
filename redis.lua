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

function LPOP(rec, bin, count, ttl)
	if (EXISTS(rec, bin)) then
		local l = rec[bin]
		local new_l = list.drop(l, count)
		rec[bin] = new_l
		local length = #new_l
		if (length == 0) then
			rec[bin .. '_size'] = nil
		else
			rec[bin .. '_size'] = length
		end
		if (ttl ~= -1) then
			record.set_ttl(rec, ttl)
		end
		UPDATE(rec)
		return list.take(l, count)
	end
	return nil
end

function LPUSH(rec, bin, value, ttl)
  local l = rec[bin]
  if (l == nil) then
    l = list()
  end
  list.prepend(l, value)
  rec[bin] = l
  local length = #l
  rec[bin .. '_size']= length
	if (ttl ~= -1) then
		record.set_ttl(rec, ttl)
	end
  UPDATE(rec)
  return length
end

local function ARRAY_RANGE (rec, bin, start, stop)
	if (EXISTS(rec, bin)) then
		local l = rec[bin]
		local switch = 0

		if (start < 0) then
			start = #l + start
			switch = switch + 1
		end

		if (stop < 0) then
			stop = #l + stop
			switch = switch + 1
		end

		if ((start > stop) and (switch == 1)) then
			local tmp = stop
			stop = start
			start = tmp
		end

		if (start == stop) then
			if (start == 0) and (#l == 0) then
				return list()
			end
			local v = l[start + 1]
			local l = list()
			list.prepend(l, v)
			return l
		elseif (start < stop) then
			local pre_list  = list.drop(l, start)
			if pre_list == nil then
			  pre_list = l
			end
			local post_list = list.take(pre_list, stop - start + 1)
			return post_list
		end
	end
	return list()
end

function LRANGE (rec, bin, start, stop)
	return ARRAY_RANGE(rec, bin, start, stop)
end

function LTRIM (rec, bin, start, stop)
	if (EXISTS(rec, bin)) then
		rec[bin] = ARRAY_RANGE(rec, bin, start, stop)
		local length = #rec[bin]
		if (length == 0) then
			rec[bin .. '_size'] = nil
		else
			rec[bin .. '_size'] = length
		end
		UPDATE(rec)
	end
	return "OK"
end

function RPOP(rec, bin, count, ttl)
	if (EXISTS(rec, bin)) then
		local l = rec[bin]
 		local result_list = nil
		if (#l <= count) then
			result_list = rec[bin]
			rec[bin .. '_size']= nil
			rec[bin] = nil
		else
      local start = #l - count
			result_list = list.drop(l, start)
			rec[bin] = list.take(l, start)
			rec[bin .. '_size']= #rec[bin]
		end
		if (ttl ~= -1) then
			record.set_ttl(rec, ttl)
		end
		UPDATE(rec)
		if (result_list ~= nil) then
			return result_list
		else
			return list()
		end
	end
	return nil
end

function RPUSH(rec, bin, value, ttl)
	local l = rec[bin]
	if (l == nil) then
		l = list()
	end
	list.append(l, value)
	rec[bin] = l
	local length = #l
	rec[bin .. '_size']= length
	if (ttl ~= -1) then
		record.set_ttl(rec, ttl)
	end
	UPDATE(rec)
	return length
end

function HSET(rec, bin, value)
	local created = 1
	if (EXISTS(rec, bin)) then
		created = 0
	end
	rec[bin] = value
	UPDATE(rec)
	return created
end

function HDEL(rec, bin)
	if (EXISTS(rec, bin)) then
		rec[bin] = nil
		UPDATE(rec)
		return 1
	end
	return 0
end

function HGETALL(rec)
	local l = list()
	if record.ttl(rec) < (MAX_INT - 60) then
		local names = record.bin_names(rec)
		for k, name in ipairs(names) do
			list.append(l, name);
			list.append(l, rec[name]);
		end
	end
	return l
end

function HINCRBY(rec, field, increment)
	if (EXISTS(rec, field)) then
		if (type(rec[field]) == "number") then
			rec[field] = rec[field] + increment
		else
			error('WRONG TYPE')
		end
	else
		rec[field] = increment;
	end
	UPDATE(rec)
	return rec[field]
end

function HMGET(rec, field_list)
	local res = list()
	for field in list.iterator(field_list) do
		list.append(res, rec[field])
	end
	return res
end

function HMSET(rec, field_value_map)
	for k,v in map.iterator(field_value_map) do
		rec[k] = v
	end
	UPDATE(rec)
	return "OK"
end
