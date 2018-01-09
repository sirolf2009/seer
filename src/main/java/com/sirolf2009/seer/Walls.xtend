package com.sirolf2009.seer

import java.io.File

import static extension com.sirolf2009.seer.retrieval.DataParser.*

class Walls {
	
	def static void main(String[] args) {
		new File("/home/floris/Documents/2018/0/1").parseFolder.filter[
			orderbook.asks.get(0).amount.doubleValue > 100 || orderbook.bids.get(0).amount.doubleValue > 100  
		].forEach[
			println(timestamp)
			println("Ask: "+orderbook.asks.get(0).amount.doubleValue)
			println("Bid: "+orderbook.bids.get(0).amount.doubleValue)
			println()
		]
	}
	
}