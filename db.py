"""
This module monitors switch, link, and host status and store the network state into the database.
To run, use ./pox.py pox.openflow.discovery pox.samples.pretty_log pox.forwarding.l3_learning pox.host_tracker db
"""

from pox.core import core
import pox.openflow.libopenflow_01 as of
from pox.openflow.discovery import Discovery
from pox.lib.util import dpid_to_str
import pox.host_tracker
import pox.lib.packet as pkt
from pox.lib.revent import *
import psycopg2

log = core.getLogger()


class db ():
    def __init__ (self):
        # Connect to Postgres
        conn_string = "host='localhost' dbname='testdb' user='fanyang' password=''"
        conn = psycopg2.connect(conn_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        global cursor
        cursor = conn.cursor()
        log.info("Connected to Database!\n")
        cursor.execute("CREATE TABLE link (id serial primary key, in_switch text, in_port text, out_switch text, out_port text);")
        cursor.execute("CREATE TABLE switch (id serial primary key, switch text, port text);")
        cursor.execute("CREATE TABLE host (id serial primary key, host_id text, switch text, port text);")
        
        
        # Listen to dependencies
        def startup ():
            core.openflow.addListeners(self, priority=0)
            core.openflow_discovery.addListeners(self)
            core.host_tracker.addListeners(self)
        core.call_when_ready(startup, ('openflow','openflow_discovery'))
    
    def _handle_LinkEvent (self, event):
        l = event.link
        if event.removed:
            log.info("Link " + l.__repr__() + " Removed")
        elif event.added:
            log.info("Link " + l.__repr__() + " Added")
            log.info("Link1: " + str(l.dpid1) + " Port: " + str(l.port1))
            log.info("Link2: " + str(l.dpid2) + " Port: " + str(l.port2))
            in_switch = str(l.dpid1)
            in_port = str(l.port1)
            out_switch = str(l.dpid2)
            out_port = str(l.port2)
            cursor.execute("INSERT INTO link (in_switch,in_port,out_switch,out_port) VALUES (%s,%s,%s,%s)",(in_switch,in_port,out_switch,out_port))

    '''def hubify (self, event):
        msg = of.ofp_flow_mod()
        msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD))
        event.connection.send(msg)
        log.info("Hubifying %s", str(event.dpid))

    #self.hubify(event) or call it with l2.learning'''
    
    def _handle_ConnectionUp (self, event):
        s_dpid = str(event.dpid)
        log.info("Switch Added: " + str(event.dpid))
        ports = event.connection.ports.__str__();
        log.info("Ports: " + ports)
        # Add all ports attached to this switch
        for val in event.connection.ports.itervalues():
            if "-eth" in val.name:
                log.info("Port number: " + str(val.port_no))
                cursor.execute("INSERT INTO switch (switch,port) VALUES (%s,%s)",(s_dpid,val.port_no))
                        conn.commit()

    def _handle_ConnectionDown (self, event):
        log.info("Switch Deleted: " + str(event.dpid))

    
    def _handle_HostEvent (self, event):
        dpid = event.entry.dpid
        port = event.entry.port
        macaddr = event.entry.macaddr
        if event.join:
            log.info("Host join: " + event.entry.__str__())
        if event.leave:
            log.info("Host leave: " + event.entry.__str__())
        if event.move:
            log.info("Host move: " + event.entry.__str__())
        
    
    '''def _handle_PacketIn(self, event):
        packet = event.parsed
        print "Got IP from: " + str(packet.src)'''


def launch ():
    core.registerNew(db)