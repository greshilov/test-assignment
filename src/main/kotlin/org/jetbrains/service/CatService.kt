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

@Service
class CatService(
    private val catRepository: CatRepository,
    private val catBreedRepository: CatBreedRepository,
    private val randomCoffeeApiClient: RandomCoffeeApiClient
) {
    private val logger: Logger = LoggerFactory.getLogger(CatService::class.java)
    private val breedCache: MutableMap<Long, String> = mutableMapOf()

    @WithSpan
    @Transactional
    fun generatePairs(catIds: List<Long>): List<CatWithBreed> {
        if (catIds.isEmpty()) {
            return emptyList()
        }

        // Batch query: load all cats in a single query
        val cats = catRepository.findAllById(catIds)
        val catMap = cats.associateBy { it.id }

        // Batch query: load all breeds in a single query instead of individual lookups
        val breedIds = cats.mapNotNull { it.breedId }.distinct()
        val breeds = if (breedIds.isNotEmpty()) {
            catBreedRepository.findAllById(breedIds).associateBy { it.id }
        } else {
            emptyMap()
        }

        // Update in-memory breed cache
        breeds.forEach { (id, breed) -> breedCache[id] = breed.name }

        // Batch HTTP requests: prepare all requests for cat-recommender-api
        val suggestRequests = cats.mapNotNull { cat ->
            val breedName = breedCache[cat.breedId] ?: breeds[cat.breedId]?.name
            if (breedName != null) {
                SuggestCatForRandomCoffeeRequest(catId = cat.id, breed = breedName)
            } else {
                null
            }
        }

        // Execute batch HTTP request instead of 43 individual calls
        if (suggestRequests.isNotEmpty()) {
            try {
                randomCoffeeApiClient.suggestCatsForRandomCoffee(suggestRequests)
            } catch (e: FeignException) {
                logger.error("Error calling cat-recommender-api for batch request", e)
            }
        }

        // Return results maintaining backward compatibility
        return cats.mapNotNull { cat ->
            val breedName = breedCache[cat.breedId] ?: breeds[cat.breedId]?.name
            if (breedName != null) {
                CatWithBreed(id = cat.id, name = cat.name, breed = breedName)
            } else {
                null
            }
        }
    }

    fun clearBreedCache() {
        breedCache.clear()
    }
}@Service
class CatService(
    private val catRepository: CatRepository,
    private val catBreedRepository: CatBreedRepository,
    private val catRecommenderClient: RandomCoffeeApiClient
) {

    private val log: Logger = LoggerFactory.getLogger(CatService::class.java)
    private val breedCache: MutableMap<Long, CatBreed> = mutableMapOf()
}fun generatePairs(limit: Int): List<Pair<CatWithBreed, CatWithBreed>> {
        val breedCache = mutableMapOf<Long, String>()
        
        val catList = catRepository.findAllWithLimit(limit).map {
            val breedName = breedCache.getOrPut(it.breedId) {
                catBreedRepository.findByIdOrNull(it.breedId)?.name ?: throw RuntimeException("Breed not found")
            }
            CatWithBreed(
                it.id,
                it.name,
                breedName
            )
        }
        
        val friendIds = catList.map { suggestCat(it) }
        val friendCats = catRepository.findAllById(friendIds)
        val friendCatsMap = friendCats.associateBy { it.id }
        
        return catList.mapIndexed { index, cat ->
            val friendId = friendIds[index]
            val friend = friendCatsMap[friendId] ?: throw RuntimeException("Friend cat not found")
            val friendBreedName = breedCache.getOrPut(friend.breedId) {
                catBreedRepository.findByIdOrNull(friend.breedId)?.name ?: throw RuntimeException("Breed not found")
            }
            cat to CatWithBreed(
                friend.id,
                friend.name,
                friendBreedName
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
        val breedsMap = catBreedRepository.findAllById(breedIds).associateBy { it.id }
        
        val result = cats.map {
            CatWithBreed(
                it.id,
                it.name,
                breedsMap[it.breedId]?.name ?: throw RuntimeException("Breed not found")
            )
        }
        return result
    }

    @WithSpan
    @Transactional
    fun getAllCats(): List<CatWithBreed> {
        val cats = catRepository.findAll()
        val breedIds = cats.map { it.breedId }.distinct()
        val breedsMap = catBreedRepository.findAllById(breedIds).associateBy { it.id }
        
        return cats.map {
            CatWithBreed(
                it.id,
                it.name,
                breedsMap[it.breedId]?.name ?: throw RuntimeException("Breed not found")
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