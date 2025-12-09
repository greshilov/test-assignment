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
data class NewCat(val name: String, val breed: String)
data class SuggestCatRequest(val cats: List<CatWithBreed>)
data class SuggestCatResponse(val recommendations: List<CatRecommendation>)
data class CatRecommendation(val catId: Long, val friendId: Long)@Serviceclass CatService(
    private val catRepository: CatRepository,
    private val catBreedRepository: CatBreedRepository,
    private val catRecommenderClient: RandomCoffeeApiClient
) {

    private val log: Logger = LoggerFactory.getLogger(CatService::class.java)fun generatePairs(limit: Int): List<Pair<CatWithBreed, CatWithBreed>> {
        // Step 1: Fetch all cats once
        val cats = catRepository.findAllWithLimit(limit)
        
        // Step 2: Batch fetch all breeds for those cats
        val breedIds = cats.map { it.breedId }.distinct()
        val breedsMap = catBreedRepository.findAllById(breedIds).associateBy { it.id }
        
        val catList = cats.map {
            CatWithBreed(
                it.id,
                it.name,
                breedsMap[it.breedId]?.name ?: throw RuntimeException("Breed not found")
            )
        }
        
        // Step 3: Batch call recommender API
        val friendIds = suggestCats(catList)
        
        // Step 4: Batch fetch friend cats
        val friendCats = catRepository.findAllById(friendIds)
        val friendCatsMap = friendCats.associateBy { it.id }
        
        // Step 5: Batch fetch friend breeds
        val friendBreedIds = friendCats.map { it.breedId }.distinct()
        val friendBreedsMap = catBreedRepository.findAllById(friendBreedIds).associateBy { it.id }
        
        // Step 6: Return results
        return catList.mapIndexed { index, cat ->
            val friendId = friendIds[index]
            val friend = friendCatsMap[friendId] ?: throw RuntimeException("Friend cat not found")
            cat to CatWithBreed(
                friend.id,
                friend.name,
                friendBreedsMap[friend.breedId]?.name ?: throw RuntimeException("Breed not found")
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
        val cats = catRepository.findAllByName(name)
        val breedIds = cats.map { it.breedId }.distinct()
        val breeds = catBreedRepository.findAllById(breedIds).associateBy { it.id }
        
        val result = cats.map {
            CatWithBreed(
                it.id,
                it.name,
                breeds[it.breedId]?.name ?: throw RuntimeException("Breed not found")
            )
        }
        return result
    }

    @WithSpan
    @Transactional
    fun getAllCats(): List<CatWithBreed> {
        val cats = catRepository.findAll()
        val breedIds = cats.map { it.breedId }.distinct()
        val breeds = catBreedRepository.findAllById(breedIds).associateBy { it.id }
        
        return cats.map {
            CatWithBreed(
                it.id,
                it.name,
                breeds[it.breedId]?.name ?: throw RuntimeException("Breed not found")
            )
        }
    }

    @WithSpan
    @Transactional
    fun countCats(): Long = catRepository.countAll()

    companion object {
        private val JACKSON_MAPPER = ObjectMapper()
    }
}data class FastAPIExceptionResponse(@JsonProperty("detail") val detail: String)class CatRecommenderIntegrationException(message: String, exception: Exception) : RuntimeException(message, exception)