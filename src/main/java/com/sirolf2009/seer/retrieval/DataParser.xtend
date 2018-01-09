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

	def static List<ITick> loadFolder(File folder) {
		folder.parseFolder.collect(Collectors.toList())
	}

	def static Stream<ITick> parseFolder(File folder) {
		loadFolderHelper(folder).sorted[a, b|a.timestamp.compareTo(b.timestamp)]
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
		file.parseFile.forEach [
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
		return new BufferedReader(new FileReader(file)).lines.map [
			val data = split(",")
			return Either.cond(data.get(0) == "o", [parseOrderbook], [parseTrade])
		]
	}

	def static ITrade parseTrade(String trade) {
		try {
			val data = trade.split(",")
			val timestamp = data.get(1).asLong
			val price = data.get(2).asDouble
			val amount = data.get(3).asDouble
			return new Trade(new Point(timestamp, price), amount)
		} catch(Exception e) {
			throw new ParseException("Failed to parse trade " + trade, e)
		}
	}

	def static IOrderbook parseOrderbook(String orderbook) {
		try {
			val data = orderbook.split(",")
			val timestamp = data.get(1).asDate
			val asks = data.get(2).parseOrders
			val bids = data.get(3).parseOrders
			return new Orderbook(timestamp, asks, bids)
		} catch(Exception e) {
			throw new ParseException("Failed to parse orderbook " + orderbook, e)
		}
	}

	def static List<ILimitOrder> parseOrders(String orders) {
		try {
			return orders.split(";").map[parseOrder].toList()
		} catch(Exception e) {
			throw new ParseException("Failed to parse orders " + orders, e)
		}
	}

	def static ILimitOrder parseOrder(String order) {
		try {
			val data = order.split(":")
			return new LimitOrder(data.get(0).asDouble, data.get(1).asDouble)
		} catch(Exception e) {
			throw new ParseException("Failed to parse order " + order, e)
		}
	}

	static class ParseException extends Exception {
		new(String msg, Throwable e) {
			super(msg, e)
		}
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
