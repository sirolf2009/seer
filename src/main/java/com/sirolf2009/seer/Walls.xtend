package com.sirolf2009.seer

import com.sirolf2009.commonwealth.trading.ITrade
import java.io.File
import java.util.Date
import java.util.LinkedList
import java.util.Queue
import java.util.concurrent.atomic.AtomicReference
import org.eclipse.xtend.lib.annotations.Data

import static extension com.sirolf2009.seer.retrieval.DataParser.*
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import java.time.Duration

class Walls {

	def static void main(String[] args) {
		val bidWall = new AtomicReference()
		val askWall = new AtomicReference()
		val tradeTracker = new TradeTracker(Duration.ofMinutes(1).toMillis())
		new File("/home/sirolf2009/Downloads/2018/0").parseFolder.forEach [
			trades.forEach[tradeTracker.add(it)]
			{
				val bidOrder = orderbook.bids.get(0)
				val bidSize = bidOrder.amount.doubleValue()
				val bidPrice = bidOrder.price.doubleValue()
				val wall = bidWall.get()
				if(bidSize > 100) {
					bidWall.set(new Wall(bidPrice, bidSize, timestamp))
				}
				if(wall !== null) {
					if(wall.price > bidPrice) {
						println(timestamp + " Bid wall breached\t" + wall+"\t"+bidPrice+" "+bidSize+" "+tradeTracker.sumBid()+" "+tradeTracker.sumAsk())
						bidWall.set(null)
					} else if(wall.price < bidPrice) {
						println(timestamp + " Bid wall held\t" + wall+"\t"+bidPrice+" "+bidSize+" "+tradeTracker.sumBid()+" "+tradeTracker.sumAsk())
					}
				}
			}
			{
				val askOrder = orderbook.asks.get(0)
				val askSize = askOrder.amount.doubleValue()
				val askPrice = askOrder.price.doubleValue()
				val wall = askWall.get()
				if(askSize > 100) {
					askWall.set(new Wall(askPrice, askSize, timestamp))
				}
				if(wall !== null) {
					if(wall.price < askPrice) {
						println(timestamp + " Ask wall breached\t" + wall+"\t"+askPrice+" "+askSize+" "+tradeTracker.sumBid()+" "+tradeTracker.sumAsk())
						askWall.set(null)
					} else if(wall.price > askPrice) {
						println(timestamp + " Ask wall held\t" + wall+"\t"+askPrice+" "+askSize+" "+tradeTracker.sumBid()+" "+tradeTracker.sumAsk())
					}
				}
			}
		]
	}

	@Data static class Wall {
		val double price
		val double amount
		val Date hit

		override toString() {
			return '''«hit» price=«price» amount=«amount»'''
		}
	}

	@FinalFieldsConstructor static class TradeTracker {
		
		val Queue<ITrade> trades = new LinkedList()
		val long timeout
		
		def add(ITrade trade) {
			trades.add(trade)
			while(trade.point.x.longValue-trades.peek().point.x.longValue > timeout) {
				trades.poll()
			}
		}
		
		def sumBid() {
			trades.map[amount.doubleValue()].filter[it > 0].reduce[a,b|a+b]
		}
		
		def sumAsk() {
			trades.map[amount.doubleValue()].filter[it < 0].map[-it].reduce[a,b|a+b]
		}
		
	}

}
