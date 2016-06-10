#!/usr/bin/env ruby

require 'rubygems'
require 'ostruct'
require 'optparse'

require 'wartslib'
require 'asfinder'

def main
    $asfinder = CAIDA::ASFinder.new ARGV[0]

    file = Warts::File.open ARGV[1]
    file.add_filters Warts::TRACE

    file.read do |trace|
        next unless trace.dest_responded?

        ippath = extract_ippath trace
        pairs = []

        ippath.each do |ip|
            if ip == nil
                pairs << ['', 'q']
            end

            asn = find_all_ases(ip)

            asn = 'r' if asn == nil
            asn = 'm' if asn.instance_of?(Array)

            pairs << [ip, asn]
        end

        monitor_ip = ippath[0][0]
        monitor_as = ippath[0][1]
        dest_ip = trace.dst
        dest_asn, pfx, len = $asfinder.get_as(dest_ip)
        rtt = trace.dest_rtt

        line = "#{monitor_ip}\t#{monitor_as}\t#{dest_ip}\t#{dest_asn}\t#{rtt}"

        pairs.each do |ip, asn|
            line.concat("\t#{ip}:#{asn}")
        end

        puts line
    end
end

# This tries to behave like skitter_as_links for edge cases.  For example,
# if there is a response from the destination, then this only returns IP
# hops up to the hop position of the response from the destination.  (Yes,
# strange enough, there can be hops past the hop position of the
# destination.)  This includes all responses at the hop position of the
# destination, even if the responses weren't from the destination.
#
# This doesn't return the trailing gap.  So you can assume that if you
# encounter a nil element before reaching the end of the result array, then
# there will always be a non-nil element before reaching the end of the
# array.
#
# This method may differ from skitter_as_links in the handling of traces with
# loops.  It's possible skitter_as_links simply ignores traces with loops
# (I need to investigate more), but this script doesn't.  However, in order
# to properly generate AS links from traces with loops, we have to ignore the
# hops in the loop.   For example, suppose the IP path looks like the
# following:
#
#     1  2  3  L  4  5  L
#
# Then the loop hops are 'L  4  5  L'.  We should ignore all hops after
# the first occurrence of L, since the 'L 4' IP link is most likely false
# (and the remaining IP links in the loop are most likely redundant anyway).
#
# --------------------------------------------------------------------------
#
# This returns an array with hop addresses at the corresponding array
# positions (the source is at index 0).  If there is more than one address
# at a given hop position, then this will use a subarray to hold all the
# addresses.  Otherwise, this stores a single address directly as a
# dotted-decimal string.  If there isn't a response at a given hop position,
# then the corresponding array location will contain a nil.
def extract_ippath(trace)
    retval = [trace.src]

    dest_response = trace.find_dest_response
    trace.each_hop_and_response do |hop, response, exists|
        next unless exists
        break if dest_response && hop > dest_response[0]

        index = hop + 1
        hop_addr = trace.hop_addr hop, response

        if retval[index]
            if retval[index].instance_of? Array
                retval[index] << hop_addr
            else
                retval[index] = [retval[index], hop_addr]
            end
        else
            retval[index] = hop_addr
        end
    end

    if trace.loops > 1 || trace.stop_reason == Warts::Trace::STOP_LOOP
        stopping_loop = find_stopping_loop(find_all_loops(retval))
        if stopping_loop
            start_index = stopping_loop[0]
            truncated_length = start_index + 1
            retval.pop while retval.length > truncated_length

            # maintain invariant that path will not end in any nil's
            retval.pop while !retval.empty? && retval[-1].nil?
        end
    end

    retval
end

# Returns an array of all loops found in the given IP path.
#
# You can apply this method to any trace, including traces without loops.
# This returns an empty array if there are no loops.  Otherwise, each array
# element specifies [ start_index, length, address ].
#
# The start index is the 0-based starting position of a loop.
# A loop that appears in adjacent hops (e.g., 'B B' in 'A B B C') has
# length 1.  A loop like 'B D B' has length 2.
#
# A path like 'A B B B' has two loops, both of length 1, starting at
# indexes 1 and 2; that is, [1, 1, B] and [2, 1, B].
#
# Multiple responses at a hop don't affect the determination of loops in
# a path.  That is, for the purposes of determining loops, it's as if there
# were only one instance of each address at any given hop.
def find_all_loops(ippath)
    retval = [] # [ [ start_index, length, address ] ]

    last_index = {} # IP address => index of last occurrence of the address
    ippath.each_with_index do |addresses, index|
        next unless addresses

        (addresses.instance_of?(Array) ? addresses.uniq : [addresses])
            .each do |address|
            if last_index[address]
                length = index - last_index[address]
                retval << [last_index[address], length, address]
            end
            last_index[address] = index
        end
    end

    retval.sort # mainly to order by starting index
end

# Finds the loop, if any, that would have caused a trace to stop for
# 'scamper -L 1 -l 1'.  This permits at most 1 loop of length 1, and no
# loops of any longer length.
#
# You can apply this method to any trace, including traces without loops.
# Returns nil if there is no stopping loop.
def find_stopping_loop(loops)
    saw_length1_loop = false
    loops.each do |loop|
        start_index, length, address = loop
        if length == 1
            return loop if saw_length1_loop
            saw_length1_loop = true
        else
            return loop
        end
    end
    nil
end

# Uses ASFinder to find the AS(es) for the given address or array of
# addresses.
#
# If a single address is given to this method and that address doesn't have
# a matching AS, then this returns nil.  If an array of addresses is given
# to this method and none of the addresses has a matching AS, then this
# returns nil.  If an array of addresses is given and an AS could be found
# for at least one address, then this returns an array of only the ASes
# that could be found (that is, this won't return an array containing nil's).
#
# NOTE: For some strange reason, a small fraction of collected traces have
#       hops with a large number of responses (hundreds or, very rarely,
#       thousands) from either the same IP address or different IP
#       addresses (that may map to different ASes).  In order to avoid
#       slowdowns in subsequent processing of AS paths, this returns only
#       the unique AS(es).
def find_all_ases(addresses)
    if addresses.instance_of? Array
        retval = []
        addresses.each_with_index do |address, _index|
            as = find_as(address)
            retval << as if as
        end

        retval.uniq!
        return nil if retval.empty?
        return (retval.length == 1 ? retval[0] : retval)
    else
        return find_as(addresses)
    end
end

def find_as(address)
    unless $special_asf.nil?
        as, prefix, len = $special_asf.get_as address
        return 's' unless as.nil?
    end
    as, prefix, len = $asfinder.get_as address
    as
end

main
