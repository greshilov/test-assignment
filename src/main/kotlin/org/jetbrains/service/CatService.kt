package org.jetbrains.service

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import feign.FeignException
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.jetbrains.recommender.RandomCoffeeApiClient
import org.jetbrains.recommender.model.SuggestCatForRandomCoffeeRequest
import org.jetbrains.repository.Cat
import org.jetbrains.repository.CatBreedRepository
import org.jetbrains.repository.CatRepository
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

data class CatWithBreed(val id: Long, val name: String, val breed: String)
data class NewCat(val name: String, val breed: String)@Serviceclass CatService(
    private val catRepository: CatRepository,
    private val catBreedRepository: CatBreedRepository,
    private val catRecommenderClient: RandomCoffeeApiClient
) {

    private val log: Logger = LoggerFactory.getLogger(CatService::class.java)fun generatePairs(limit: Int): List<Pair<CatWithBreed, CatWithBreed>> {
        val cats = catRepository.findAllWithLimit(limit)
        
        // Collect all cat breed IDs
        val catBreedIds = cats.map { it.breedId }.distinct()
        
        // Batch fetch all cat breeds
        val catBreeds = catBreedRepository.findAllById(catBreedIds).associateBy { it.id }
        
        // Create CatWithBreed objects for main cats
        val catList = cats.map {
            CatWithBreed(
                it.id,
                it.name,
                catBreeds[it.breedId]?.name ?: throw RuntimeException("Breed not found")
            )
        }
        
        // Collect all friend IDs
        val friendIds = catList.map { cat -> suggestCat(cat) }
        
        // Batch fetch all friend cats
        val friendCats = catRepository.findAllById(friendIds).associateBy { it.id }
        
        // Collect all friend breed IDs
        val friendBreedIds = friendCats.values.map { it.breedId }.distinct()
        
        // Batch fetch all friend breeds
        val friendBreeds = catBreedRepository.findAllById(friendBreedIds).associateBy { it.id }
        
        // Map results in-memory
        return catList.mapIndexed { index, cat ->
            val friendId = friendIds[index]
            val friend = friendCats[friendId] ?: throw RuntimeException("Friend cat not found")
            cat to CatWithBreed(
                friend.id,
                friend.name,
                friendBreeds[friend.breedId]?.name ?: throw RuntimeException("Breed not found")
            )
        }
    }@Transactional
    fun addCat(cat: NewCat): CatWithBreed {
        val breed = catBreedRepository.findByName(cat.breed).orElseThrow { RuntimeException("Breed not found") }
        val createdCat = catRepository.save(Cat(0L, breed.id, cat.name, ""))
        return CatWithBreed(createdCat.id, createdCat.name, breed.name)
    }@WithSpan
    private fun suggestCat(cat: CatWithBreed): Long =
        try {
            catRecommenderClient.suggestCat(SuggestCatForRandomCoffeeRequest(cat.id, cat.name, cat.breed)).id
        } catch (e: FeignException) {
            log.error("Failed to suggest cat", e)
            throw CatRecommenderIntegrationException(
                JACKSON_MAPPER.readValue(e.contentUTF8(), FastAPIExceptionResponse::class.java).detail,
                e
            )
        }@WithSpan
    @Transactional
    fun findCatsByName(name: String): List<CatWithBreed> {
        val result = catRepository.findAllByName(name).map {
            CatWithBreed(
                it.id,
                it.name,
                catBreedRepository.findByIdOrNull(it.breedId)?.name ?: throw RuntimeException("Breed not found")
            )
        }
        return result
    }@WithSpan
    @Transactional
    fun getAllCats(): List<CatWithBreed> =
        catRepository.findAll().map {
            CatWithBreed(
                it.id,
                it.name,
                catBreedRepository.findByIdOrNull(it.breedId)?.name ?: throw RuntimeException("Breed not found")
            )
        }

    @WithSpan
    @Transactional
    fun countCats(): Long = catRepository.countAll()

    companion object {
        private val JACKSON_MAPPER = ObjectMapper()
    }
}data class FastAPIExceptionResponse(@JsonProperty("detail") val detail: String)class CatRecommenderIntegrationException(message: String, exception: Exception) : RuntimeException(message, exception)