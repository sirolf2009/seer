package com.sirolf2009.seer.retrieval

import com.sirolf2009.commonwealth.ITick
import com.sirolf2009.commonwealth.Tick
import com.sirolf2009.commonwealth.timeseries.Point
import com.sirolf2009.commonwealth.trading.ITrade
import com.sirolf2009.commonwealth.trading.Trade
import com.sirolf2009.commonwealth.trading.orderbook.ILimitOrder
import com.sirolf2009.commonwealth.trading.orderbook.IOrderbook
import com.sirolf2009.commonwealth.trading.orderbook.LimitOrder
import com.sirolf2009.commonwealth.trading.orderbook.Orderbook
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.util.ArrayList
import java.util.Date
import java.util.List
import java.util.stream.Collectors
import java.util.stream.Stream
import com.sirolf2009.seer.model.Either

class DataParser {
	
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
	
	def static List<ITick> loadFolder(File folder) {
		folder.parseFolder.collect(Collectors.toList())
	}
	
	def static Stream<ITick> parseFolder(File folder) {
		loadFolderHelper(folder).sorted[a,b| a.timestamp.compareTo(b.timestamp)]
	}
	
	def static private Stream<ITick> loadFolderHelper(File file) {
		if(file.directory) {
			file.listFiles.parallelStream.flatMap[loadFolderHelper]
		} else {
			return loadFile(file).stream()
		}
	}
	
	def static List<ITick> loadFile(File file) {
		val trades = new ArrayList()
		val ticks = new ArrayList()
		file.parseFile.forEach[
			consume([
				ticks.add(new Tick(timestamp, it, new ArrayList(trades)))
				trades.clear()
			], [
				trades.add(it)
			])
		]
		return ticks
	}
	
	def static Stream<Either<IOrderbook, ITrade>> parseFile(File file) {
		return new BufferedReader(new FileReader(file)).lines.map[
			val data = split(",")
			return Either.cond(data.get(0) == "t", [parseOrderbook], [parseTrade])
		]
	}
	
	def static ITrade parseTrade(String trade) {
		val data = trade.split(",")
		val timestamp = data.get(1).asLong
		val price = data.get(2).asDouble
		val amount = data.get(3).asDouble
		return new Trade(new Point(timestamp, price), amount)
	}
	
	def static IOrderbook parseOrderbook(String orderbook) {
		val data = orderbook.split(",")
		val timestamp = data.get(1).asDate
		val asks = data.get(2).parseOrders
		val bids = data.get(3).parseOrders
		return new Orderbook(timestamp, asks, bids)
	}
	
	def static List<ILimitOrder> parseOrders(String orders) {
		return orders.split(";").map[parseOrder].toList()
	}
	
	def static ILimitOrder parseOrder(String order) {
		val data = order.split(":")
		return new LimitOrder(data.get(0).asDouble, data.get(1).asDouble)
	}
	
	def static asDouble(String string) {
		return Double.parseDouble(string)
	}
	
	def static asDate(String string) {
		return new Date(string.asLong)
	}
	
	def static asLong(String string) {
		return Long.parseLong(string)
	}
	
}
