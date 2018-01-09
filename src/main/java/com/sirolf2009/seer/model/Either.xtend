package com.sirolf2009.seer.model

import java.util.function.Consumer
import java.util.function.Supplier
import org.eclipse.xtend.lib.annotations.EqualsHashCode
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor @EqualsHashCode class Either<A, B> {
	
	val A a
	val B b
	
	def isA() {
		return a !== null
	}
	
	def isB() {
		return !isA()
	}
	
	def getA() {
		return a
	}
	
	def getB() {
		return b
	}
	
	def consume(Consumer<A> consumerA, Consumer<B> consumerB) {
		if(isA()) {
			consumerA.accept(a)
		} else {
			consumerB.accept(b)
		}
	}
	
	def static <A,B> cond(boolean condition, Supplier<A> aSupplier, Supplier<B> bSupplier) {
		if(condition) {
			return new Either(aSupplier.get(), null)
		} else {
			return new Either(null, bSupplier.get())
		}
	}
	
}