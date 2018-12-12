#!/usr/bin/python 

import ldap
import sys
from argparse import ArgumentParser

DEFAULT_DN='cn=Directory Manager'
DEFAULT_PWD='******'

# parse input arguments
def parse_args():
	parser = ArgumentParser(description='Check IPA replication status')

	parser.add_argument('-u', required=True, help="ldap uri (e.g. ldaps://ipaserver.io)", dest="uri")
	parser.add_argument('-z', required=True, help="ldap binddc (e.g. 'dc=north' or 'dc=east' or 'dc=milan,dc=mi')", dest="binddc")
	parser.add_argument('-d', required=False, help="ldap binddn (Default cn=Directory Manager)", dest="binddn", default=DEFAULT_DN)
	parser.add_argument('-p', required=False, help="ldap bind password", dest="bindpw", default=DEFAULT_PWD)
	parser.add_argument('-v', required=False, help="run in verbose mode (diagnostics to standard output)", action="store_true", dest="verbose")
	
	if len(sys.argv) <= 1:
		parser.print_help()
		sys.exit(2)

	args = parser.parse_args()
	return args

def get_replica_agreements(ldap_conn):
	return ldap_conn.search_s('cn=config', ldap.SCOPE_SUBTREE, '(objectclass=nsDS5ReplicationAgreement)', ['nsDS5ReplicaHost', 'nsds5replicaLastUpdateStatus', 'nsds5replicaLastUpdateStart', 'nsds5replicaLastUpdateEnd'])
	
def get_replica_conflicts(ldap_conn, binddc):
	return ldap_conn.search_s(binddc, ldap.SCOPE_SUBTREE, '(nsds5ReplConflict=*)', ['nsds5ReplConflict'])

def get_masters(ldap_conn, binddc):
	records = ldap_conn.search_s("ou=profile," + binddc, ldap.SCOPE_SUBTREE, "defaultServerList=*", ['defaultServerList'])
	masters_str = records[0][1]['defaultServerList'][0]
	return masters_str.split(' ')

def count_entities(ldap_conn, binddc):

	clist = []

	hosts     = ldap_conn.search_s('cn=computers,cn=accounts,' + binddc, ldap.SCOPE_SUBTREE, '(fqdn=*)', ['dn'])
	users     = ldap_conn.search_s('cn=users,cn=accounts,' + binddc, ldap.SCOPE_SUBTREE, '(cn=*)', ['sn'])
	groups    = ldap_conn.search_s('cn=groups,cn=accounts,' + binddc, ldap.SCOPE_SUBTREE, '(cn=*)', ['cn'])
	hgroups   = ldap_conn.search_s('cn=hostgroups,cn=accounts,' + binddc, ldap.SCOPE_SUBTREE, '(cn=*)', ['cn'])
	hbacs     = ldap_conn.search_s(binddc, ldap.SCOPE_SUBTREE, '(objectClass=ipahbacrule)', ['cn'])
	sudorules = ldap_conn.search_s('cn=sudorules,cn=sudo,' + binddc, ldap.SCOPE_SUBTREE, '(cn=*)', ['cn'])
	sudocmds  = ldap_conn.search_s('cn=sudocmds,cn=sudo,' + binddc, ldap.SCOPE_SUBTREE, '(sudoCmd=*)', ['sudoCmd'])
	sudogrps  = ldap_conn.search_s('cn=sudocmdgroups,cn=sudo,' + binddc, ldap.SCOPE_SUBTREE, '(cn=*)', ['cn'])
	idnsnames = ldap_conn.search_s('cn=dns,' + binddc, ldap.SCOPE_SUBTREE, '(objectClass=*)', ['dn'])

	if verbose:
		print "** hosts: %s\n** users: %s\n** groups: %s\n** hostgroups: %s		\
			\n** hbacrules: %s\n** sudorules: %s\n** sudocmds: %s\n** sudogroups: %s	\
			\n** dnsnames: %s\n" % \
			(len(hosts), len(users), len(groups), len(hgroups), \
			len(hbacs), len(sudorules), len(sudocmds), len(sudogrps), len(idnsnames))

	clist.append(len(hosts))
	clist.append(len(users))
	clist.append(len(groups))
	clist.append(len(hgroups))
	clist.append(len(hbacs))
	clist.append(len(sudorules))
	clist.append(len(sudocmds))
	clist.append(len(sudogrps))
	clist.append(len(idnsnames))

	return clist


#### MAIN ####
args = parse_args()

try:
	binddc = args.binddc.strip()
	verbose = args.verbose

	# inizialize ldap connection
	l = ldap.initialize(args.uri)
	if args.bindpw:
		l.bind_s(args.binddn, args.bindpw)

	# get replica agreements
	replica_agreements = get_replica_agreements(l)

	if not len(replica_agreements):
		print "ERROR in replication: no replicas found"
		sys.exit(2)

	# loop through replication agreements
	summary = ''
	for rhost in replica_agreements:
		summary += "Replica to %s: status %s\n" % (rhost[1]['nsDS5ReplicaHost'][0], rhost[1]['nsds5replicaLastUpdateStatus'][0])
		status = int(filter(str.isdigit, rhost[1]['nsds5replicaLastUpdateStatus'][0]))

		if verbose: print summary
        	# status=0 REPLICA STATE OK
        	# status=1 REPLICA STATE BUSY
        	if status not in [0, 1]:
				print "ERROR in replication: %s" % (summary)
				sys.exit(2)
				
	# get ldap conficts
	conflicts = get_replica_conflicts(l, binddc)
	if conflicts and type(conflicts) is list:
		print "ERROR in replication: found %s conflicts" % (len(conflicts))
		sys.exit(2)

	# count ldap user/host entries and compare them with th other masters	
	masters = get_masters(l, binddc)

	entities_counter = {}
	prev_master = ''
	for master in masters:
		l = ldap.initialize("ldaps://"+master)
		if args.bindpw:
			l.bind_s(args.binddn, args.bindpw)

		if verbose: print "Getting objects from", master

		# get a list of counters for user/host entries
		entities_counter[master] = count_entities(l, binddc)

		# compare objects counters
		if prev_master == '':
			prev_master = master
			continue
		if entities_counter[master] == entities_counter[prev_master]:
			continue
		else:
			print "ERROR in replication: different objects count between %s and %s" % (master, prev_master)
			sys.exit(2)

		prev_master = master

	#values = entities_counter.values()
	#healthy = all( values[i] == values[i+1] for i in range(values)-1 )
	print "LDAP status is OK"	
	sys.exit(0)

except Exception, e:
    print "ERROR in replication: %s" % (e)
    sys.exit(2)

finally:
    l.unbind()

sys.exit(3)
